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
    const PAIR_ROWS = 600;
    const hue = j => Math.round((j * 137.508 + 215) % 360);
    const cc = j => `style="--cc:hsl(${hue(j)} 62% var(--xp-l))"`;
    const parts = fid => {
        const [coll, rest = ''] = fid.split(':func:');
        const i = rest.indexOf(':');
        return { coll, md5: rest.slice(0, i), addr: rest.slice(i + 1) };
    };

    class NwayFunction {
        constructor(el, o) {
            this.el = el;
            this.source = o.source || {};
            this.doc = o.doc || {};
            this.fids = o.fids;
            this.name = o.name || '';
            this.onState = o.onState || null;
            this.embedded = !!o.embedded;
            this.onLoad = o.onLoad || null;
            this.s = { tab: 'facets', only: false, pair: null, cv: 'blocks', open: {}, fn: null, single: false, ...(o.state || {}) };
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
            // Side-by-side code panes scroll together (scroll does not bubble, so capture it).
            el.addEventListener('scroll', e => {
                const pre = e.target;
                if (!pre.matches || !pre.matches('pre') || this._syncing) return;
                const box = pre.closest('.xp-panes, .xp-frow');
                if (!box) return;
                this._syncing = true;
                box.querySelectorAll('pre').forEach(o => { if (o !== pre) { o.scrollTop = pre.scrollTop; o.scrollLeft = pre.scrollLeft; } });
                requestAnimationFrame(() => { this._syncing = false; });
            }, true);
            el.addEventListener('keydown', e => {
                if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('[data-xfnref],[data-xfile],[data-xcall]')) this._click(e);
            });
        }

        destroy() {
            this._dead = true;
        }

        _qs() {
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries({ ...this.source, ...this.doc })) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            qs.set('fids', this.fids.join(','));
            return qs;
        }

        _persist() {
            if (!this.onState) return;
            const { tab, cv, fn, only, single } = this.s;
            this.onState({ tab, cv, fn, only, single });
        }

        async load() {
            this.el.innerHTML = `<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Comparing ${this.fids.length} functions...</div>`;
            try {
                const res = await fetch(`/api/bin_sim/nway/facets?${this._qs().toString()}`);
                const body = await res.json().catch(() => ({}));
                if (!res.ok) throw new Error(body.message || body.error || body.detail || `HTTP ${res.status}`);
                if (this._dead) return;
                this.data = body;
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
            if (s.tab === 'code' && !code) s.tab = 'facets';
            const tab = (k, label, why) => `<button class="bsim-tab${s.tab === k ? ' active' : ''}" role="tab" data-xtab="${k}"${why ? ` title="${escapeAttr(why)}"` : ''}>${label}</button>`;
            const nf = d.functions.length;
            this.el.innerHTML = `
                ${this.embedded ? '' : `<div class="nway-head"><h2><i class="fa-solid fa-table-columns"></i> ${escapeHtml(this.name || 'Function family')} <span class="exp-badge" title="Experimental: results and layout may change"><i class="fa-solid fa-flask"></i> Experimental</span></h2>
                    <p>${nf} functions, one per file. Strings, calls, constants and code compared across them.</p></div>`}
                <div class="nway-card xp-head"><div class="xp-cols">${d.functions.map((f, j) => this._card(f, j)).join('')}</div>
                    <div><div class="xp-mx" style="--n:${nf}">${this._matrix()}</div>
                    <div class="xp-cap">Function similarity from the matcher; a dash means no match was found. Click a cell to open that pair in the 2-way diff.</div></div></div>
                <div class="xp-bar"><div class="bsim-tabbar" role="tablist" style="margin:0">
                    ${this.embedded ? '' : tab('simple', 'Simple')}
                    ${this.embedded ? '' : tab('facets', 'Facets')}
                    ${this.embedded ? '' : tab('code', 'Code blocks', !code ? `Code blocks need at most ${d.limits.code} functions (this row has ${nf})` : '')}
                    ${this.embedded ? '' : tab('pair', 'Pair diff')}</div><span class="xp-sp"></span>
                    ${s.tab === 'pair' || s.tab === 'simple' ? '' : `<label class="xp-chk"><input type="checkbox" data-xonly ${s.only ? 'checked' : ''}> Differences only</label>`}</div>
                <div class="xp-body">${this._body()}</div>`;
            this._loadSummaries();
            if (s.tab === 'pair') this._loadPair();
            if (s.tab === 'code') this._loadTitles();
            if (s.tab === 'simple') this._loadSimple();
        }

        _body() {
            const t = this.s.tab;
            return t === 'simple' ? this._simple() : t === 'facets' ? this._facets() : t === 'code' ? this._code() : this._pair();
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
                this.simpleCode = (data.rows || []).map(row =>
                    `<div class="code-line"><div class="gutter" contenteditable="false"><div class="line-num">${Number(row.line_idx)}</div></div><div class="line-content">${row.tokens.map(t => window.renderTokenHtml(t)).join('')}</div></div>`
                ).join('') || 'No code available.';
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
        }

        _funcRef(j, cls = 'xp-badge', link = true) {
            const f = this.data.functions[j];
            return `<span class="${cls}${link ? ' xp-entity-link' : ''}" ${cc(j)}${link ? ` data-xfnref="${j}" role="link" tabindex="0" title="${escapeAttr(`Open function ${f.name || f.addr}`)}"` : ''}>${escapeHtml(f.letter)}</span>`;
        }

        _fileRef(j, cls = 'xp-fn', link = true) {
            const f = this.data.functions[j], { md5 } = parts(this.fids[j] || '');
            return `<span class="${cls}${link ? ' xp-entity-link' : ''}"${link ? ` data-xfile="${j}" role="link" tabindex="0" title="${escapeAttr(`${f.file_name || md5} (${md5})`)}"` : ''}>${escapeHtml(f.file_name || md5)}</span>`;
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
                    h += `<button class="xp-v" data-xpair="${r},${c}"${bg} title="${escapeAttr(`${fs[r].letter} vs ${fs[c].letter}: ${v == null ? 'no match' : v.toFixed(2)}`)}">${v == null ? '&ndash;' : escapeHtml(v.toFixed(2).slice(1))}</button>`;
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
            return `<p class="xp-hint">Every string, API call, callee and constant found in the ${d.functions.length} functions, one list per kind. A dot means the function has it; items missing from some functions are marked in amber.</p>
                ${d.facets.map(f => this._facet(f)).join('')}
                <div class="xp-grp"><h3>Shape<span class="n">amber = more than 20% from the median</span></h3><div class="xp-fx"><table>${this._thead()}${shape}</table></div></div>`;
        }

        // ---- Code blocks ----
        static GLYPH = { same: '=', mod: '~', moved: '⇄', restr: '◧', miss: '✕', extra: '+' };
        static SNAME = { same: 'identical', mod: 'modified', moved: 'moved', restr: 'restructured', miss: 'missing', extra: 'only here' };

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

        _isSame(b) { return Object.values(b.cells).every(c => c.st === 'same'); }

        _code() {
            const s = this.s;
            const sw = `<div class="xp-cbar"><div class="view-toggle" role="group" aria-label="Code view"><button class="view-btn${s.cv === 'blocks' ? ' active' : ''}" data-xcv="blocks">Blocks</button><button class="view-btn${s.cv === 'full' ? ' active' : ''}" data-xcv="full">Full function</button></div>
                <span class="xp-hint" style="margin:0">${s.cv === 'blocks' ? 'Overview of what changed, block by block.' : 'Read the whole function, aligned to the reference by block.'}</span>
                <button class="xp-aibtn" data-xask title="Open AI Insight and ask it about this whole row"><i class="fa-solid fa-robot"></i> Ask AI about this row</button>
                <span class="xp-naming" ${this._naming ? '' : 'hidden'}><i class="fa-solid fa-spinner fa-spin"></i> naming blocks&hellip;</span></div>`;
            return sw + (s.cv === 'blocks' ? this._blocksView() : this._fullView());
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
                        slots.get(after).push({ seg, x: c.extras.find(e => e.id === seg.x) });
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

        // One row grid for both graphs: matched blocks share a row, so equal code lines up even with
        // extra blocks between. Back edges (loops) are left out of the layering and drawn as side arcs.
        _cfgGrid(R, F, pairOf) {
            const fid = k => (pairOf.has(k) ? 'r' + pairOf.get(k) : 'f' + k);
            const ids = [...R.nodes.keys()].map(i => 'r' + i).concat([...F.nodes.keys()].map(fid));
            const adj = new Map(ids.map(id => [id, new Set()]));
            const rE = R.edges.map(([u, v]) => ['r' + u, 'r' + v]), fE = F.edges.map(([u, v]) => [fid(u), fid(v)]);
            // A block outside the CFG (the signature) hangs above the next block in source order.
            [[R, rE, i => 'r' + i], [F, fE, fid]].forEach(([G, E, idf]) => {
                const used = new Set(E.flat());
                G.nodes.forEach((nd, i) => { if (!used.has(idf(i)) && i + 1 < G.nodes.length) E.push([idf(i), idf(i + 1)]); });
            });
            for (const [u, v] of rE.concat(fE)) if (u !== v) adj.get(u).add(v);
            const state = new Map(), back = new Set();
            for (const root of ['r0', ...adj.keys()]) {
                if (state.has(root)) continue;
                const stack = [[root, [...adj.get(root)]]];
                state.set(root, 1);
                while (stack.length) {
                    const top = stack[stack.length - 1], v = top[1].shift();
                    if (v === undefined) { state.set(top[0], 2); stack.pop(); continue; }
                    if (state.get(v) === 1) back.add(top[0] + '|' + v);
                    else if (!state.has(v)) { state.set(v, 1); stack.push([v, [...adj.get(v)]]); }
                }
            }
            const indeg = new Map(ids.map(id => [id, 0])), row = new Map(ids.map(id => [id, 0]));
            for (const [u, vs] of adj) for (const v of vs) if (!back.has(u + '|' + v)) indeg.set(v, indeg.get(v) + 1);
            const q = ids.filter(id => !indeg.get(id));
            for (let h = 0; h < q.length; h++) for (const v of adj.get(q[h])) {
                if (back.has(q[h] + '|' + v)) continue;
                row.set(v, Math.max(row.get(v), row.get(q[h]) + 1));
                indeg.set(v, indeg.get(v) - 1);
                if (!indeg.get(v)) q.push(v);
            }
            const NW = 44, mean = xs => xs.reduce((t, v) => t + v, 0) / xs.length;
            // Each graph keeps its own shape. Edges spanning several rows get a dummy per row so they
            // route around blocks; rows are ordered by barycenter sweeps, then packed under their neighbours.
            const place = (members, edges) => {
                const rowOf = new Map(row), dn = new Map(), up = new Map(), chain = new Map(), ends = new Map(), seen = new Set(), ids = [...members];
                const link = (u, v) => { (dn.get(u) || dn.set(u, []).get(u)).push(v); (up.get(v) || up.set(v, []).get(v)).push(u); };
                for (const [u, v] of edges) {
                    const key = u + '|' + v;
                    if (u === v || back.has(key) || seen.has(key)) continue;
                    seen.add(key);
                    let prev = u;
                    const ds = [];
                    for (let r = row.get(u) + 1; r < row.get(v); r++) { const d = `d${key}#${r}`; rowOf.set(d, r); ids.push(d); link(prev, d); prev = d; ds.push(d); }
                    link(prev, v);
                    chain.set(key, ds);
                    ds.forEach((d, i) => ends.set(d, [u, v, (i + 1) / (ds.length + 1)]));
                }
                const byRow = new Map(), pos = new Map();
                ids.forEach(id => { const r = rowOf.get(id); (byRow.get(r) || byRow.set(r, []).get(r)).push(id); });
                const asc = [...byRow.keys()].sort((m, n) => m - n), desc = [...asc].reverse();
                const reindex = r => byRow.get(r).forEach((id, i) => pos.set(id, i));
                asc.forEach(reindex);
                const sweep = (rs, nb) => rs.forEach(r => {
                    const arr = byRow.get(r).map(id => { const ns = nb.get(id) || []; return { id, b: ns.length ? mean(ns.map(n => pos.get(n))) : pos.get(id) }; });
                    arr.sort((m, n) => m.b - n.b);
                    byRow.set(r, arr.map(m => m.id));
                    reindex(r);
                });
                for (let it = 0; it < 4; it++) { sweep(asc, up); sweep(desc, dn); }
                const hw = id => (id[0] === 'd' ? 4 : NW / 2), x = new Map();
                const pack = (rs, nbf) => rs.forEach(r => {
                    const arr = byRow.get(r);
                    let root = 0;
                    const want = arr.map(id => {
                        const e = ends.get(id);
                        if (e && x.has(e[0]) && x.has(e[1])) return x.get(e[0]) + (x.get(e[1]) - x.get(e[0])) * e[2];
                        const ns = nbf(id).filter(n => x.has(n));
                        return ns.length ? mean(ns.map(n => x.get(n))) : x.has(id) ? x.get(id) : (root += 60) - 60;
                    });
                    const at = [];
                    want.forEach((w, i) => at.push(i ? Math.max(w, at[i - 1] + hw(arr[i - 1]) + hw(arr[i]) + 14) : w));
                    const shift = mean(want.map((w, i) => w - at[i]));
                    arr.forEach((id, i) => x.set(id, at[i] + shift));
                });
                // First a top-down pass under the parents, then passes that pull each block towards all its neighbours.
                const par = id => up.get(id) || [], all = id => par(id).concat(dn.get(id) || []);
                pack(asc, par);
                for (let it = 0; it < 8; it++) { pack(desc, all); pack(asc, all); }
                const lo = Math.min(...[...x].map(([id, v]) => v - hw(id)));
                for (const [id, v] of x) x.set(id, v - lo);
                return { x, rowOf, chain, w: Math.max(...[...x].map(([id, v]) => v + hw(id))) };
            };
            const L = place(ids.filter(id => id[0] === 'r' && +id.slice(1) < R.nodes.length), rE);
            const M = place([...F.nodes.keys()].map(fid).filter((id, i, a) => a.indexOf(id) === i), fE);
            return { fid, row, back, L, M, rows: Math.max(...row.values()) + 1 };
        }

        // Reference CFG left, one function's CFG right; matched blocks are linked and coloured by state.
        _cfgPair(j, c) {
            const R = c.cfg[String(c.reference)], F = c.cfg[String(j)];
            if (R.nodes.length > 80 || F.nodes.length > 80) return '';
            const entries = c.funcs[String(j)] || [], pairOf = new Map(), taken = new Set();
            F.nodes.forEach((nd, k) => {
                const ref = entries[k]?.refs?.[0], i = ref ? c.blocks.findIndex(b => b.id === ref.b) : -1;
                if (i >= 0 && !taken.has(i)) { taken.add(i); pairOf.set(k, i); }
            });
            const g = this._cfgGrid(R, F, pairOf), NW = 44, NH = 22, ROW = 46, gap = 50;
            const off = g.L.w + gap, W = off + g.M.w, H = g.rows * ROW;
            const cx = (side, id) => (side ? off + g.M.x.get(id) : g.L.x.get(id));
            const at = (side, id) => ({ x: cx(side, id) - NW / 2, y: g.row.get(id) * ROW });
            const edge = (side, u, v) => {
                const key = u + '|' + v, G = side ? g.M : g.L;
                if (g.back.has(key)) { const a = at(side, u), b = at(side, v), s = 26; return `<path class="e back" d="M${a.x + NW},${a.y + NH / 2} C${a.x + NW + s},${a.y + NH / 2} ${b.x + NW + s},${b.y + NH / 2} ${b.x + NW},${b.y + NH / 2}"/>`; }
                let pts = [{ x: cx(side, u), y: g.row.get(u) * ROW + NH }]
                    .concat((G.chain.get(key) || []).map(d => ({ x: (side ? off : 0) + G.x.get(d), y: G.rowOf.get(d) * ROW + NH / 2 })))
                    .concat([{ x: cx(side, v), y: g.row.get(v) * ROW }]);
                // Drop points on a straight run, then draw straight pieces with rounded corners.
                pts = pts.filter((q, i) => !i || i === pts.length - 1 || Math.abs((q.x - pts[i - 1].x) * (pts[i + 1].y - q.y) - (q.y - pts[i - 1].y) * (pts[i + 1].x - q.x)) > 1);
                if (pts.length === 2) { const m = (pts[0].y + pts[1].y) / 2; return `<path class="e" d="M${pts[0].x},${pts[0].y} C${pts[0].x},${m} ${pts[1].x},${m} ${pts[1].x},${pts[1].y}"/>`; }
                let d = `M${pts[0].x},${pts[0].y}`;
                for (let i = 1; i < pts.length - 1; i++) {
                    const a = pts[i - 1], q = pts[i], b = pts[i + 1], la = Math.hypot(q.x - a.x, q.y - a.y), lb = Math.hypot(b.x - q.x, b.y - q.y), r = Math.min(12, la / 2, lb / 2);
                    d += ` L${q.x + (a.x - q.x) * r / la},${q.y + (a.y - q.y) * r / la} Q${q.x},${q.y} ${q.x + (b.x - q.x) * r / lb},${q.y + (b.y - q.y) * r / lb}`;
                }
                return `<path class="e" d="${d} L${pts[pts.length - 1].x},${pts[pts.length - 1].y}"/>`;
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
                <div class="xp-cfg" style="width:${W}px;height:${H}px"><svg width="${W}" height="${H}">${svg}</svg>${nodes}</div></div>`;
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
            return `<div class="xp-cfgs"><p class="xp-hint">Basic blocks from Ghidra, merged along single edges. Matched blocks are linked and coloured like the table below; + marks a block with no counterpart. Click a block to jump to its code.</p>${js.map(j => this._cfgPair(j, c)).join('')}</div>`;
        }

        _blockBody(b) {
            const d = this.data, c = d.consensus;
            if (!b.groups.length) return `<div class="xp-bd"><p class="xp-hint" style="margin:0">Identical in all ${d.functions.length} functions.</p><div class="xp-panes"><div class="xp-pane"><div class="xp-ph">${this._funcRef(c.reference)} all functions</div><pre>${this._codeHtml(b.ref_syntax, null, null, c.reference)}</pre></div></div></div>`;
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
            const head = `<p class="xp-hint">When decompiled code is laid out differently, lines do not match one to one. Functions are cut into blocks (loops, branches, straight runs) and each block is matched by its content, in any order. Each cell shows how that block looks in the other function: how similar it is, or that it moved, was merged into or split from another block, or is gone. Click a cell or a block to compare it with the reference.</p>`;
            const legend = `<div class="xp-legend">${Object.keys(NwayFunction.SNAME).map(k => `<span><span class="xp-cell st-${k}" style="min-width:0;height:18px;cursor:default">${G[k]}</span>${NwayFunction.SNAME[k]}</span>`).join('')}</div>`;
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
                const verdict = `<span class="xp-sum st-${worst}" title="${escapeAttr(Object.keys(NwayFunction.SNAME).filter(k => k !== 'same' && sts.some(x => x.st === k)).map(k => `${sts.filter(x => x.st === k).length} ${NwayFunction.SNAME[k]}`).join(', ') || 'identical in all')}">${worst === 'same' ? G2.same : G2[worst]} ${nSame} of ${sts.length} match</span>${sims.length ? `<span class="xp-min" title="lowest similarity of the modified or moved functions">${Math.round(Math.min(...sims) * 100)}%<small> min</small></span>` : ''}`;
                return `<div class="xp-blk wst-${worst}"><button class="xp-bh" data-xb="${escapeAttr(b.id)}" aria-expanded="${open}"><span class="xp-bid">${escapeHtml(b.id)}</span>${this._tspan(b)}${verdict}</button>${open ? this._blockBody(b) : ''}</div>`;
            }).join('');
            const hidden = only ? c.blocks.length - ordered.filter(item => item.b).length : 0;
            return head + this._cfgStrip() + this._ribbon(ordered) + legend + accordion
                + (hidden ? `<p class="xp-foot">${hidden} block${hidden === 1 ? '' : 's'} identical in all ${this.data.functions.length} functions hidden.</p>` : '')
                + (c.truncated ? `<p class="xp-foot">Large functions are compared on their first ${Number(this.data.limits.lines)} lines and ${Number(this.data.limits.blocks)} blocks.</p>` : '');
        }

        _band(id, st, sim, title, extra = '', tail = '') {
            return `<div class="xp-fband"><span class="xp-bid">${escapeHtml(id)}</span><span class="xp-cell st-${st}" style="cursor:default">${NwayFunction.GLYPH[st]} ${NwayFunction.SNAME[st]}${st === 'miss' || st === 'same' || st === 'extra' ? '' : ' ' + Math.round(sim * 100) + '%'}</span>${extra}<span class="dim">${escapeHtml(title)}</span><i></i>${tail}</div>`;
        }

        _fullGroups(b, f) {
            const same = this._others().filter(j => this._cell(b, j).st === 'same');
            const groups = [...(same.length ? [{ funcs: same, st: 'same' }] : []), ...b.groups];
            return `<div class="xp-vchips xp-full-groups"><span class="lab">Variants</span>${groups.map(g => `<button class="xp-chip" aria-pressed="${g.funcs.includes(f)}" data-xfn="${g.funcs.includes(f) ? f : g.funcs[0]}">${g.funcs.map(j => this._funcRef(j, 'xp-badge', false)).join('')} ${NwayFunction.GLYPH[g.st]} ${NwayFunction.SNAME[g.st]}</button>`).join('')}</div>`;
        }

        _functionGroups(single) {
            const c = this.data.consensus, groups = new Map();
            this.data.functions.forEach((fn, j) => {
                if (j === c.reference && !single) return;
                const text = j === c.reference ? c.blocks.map(b => b.ref_text).join('\n')
                    : (c.funcs[String(j)] || []).map(seg => seg.text.map(([t]) => t).join('')).join('\n');
                // Use the matcher's whitespace and generated-address normalization.
                const normalized = text.trim().replace(/\s+/g, ' ').replace(/\b(FUN|DAT|LAB|PTR|SUB|UNK|switchD|caseD)_[0-9a-fA-F]+/g, '$1_');
                const key = normalized && !c.truncated ? normalized : j;
                if (!groups.has(key)) groups.set(key, []);
                groups.get(key).push(j);
            });
            return [...groups.values()];
        }

        _fullView() {
            const d = this.data, c = d.consensus, s = this.s, single = s.single;
            if (s.fn == null || (!single && s.fn === c.reference)) s.fn = this._others()[0];
            const f = s.fn;
            const chips = this._functionGroups(single).map(g => `<button class="xp-chip" aria-pressed="${g.includes(f)}" data-xfn="${g.includes(f) ? f : g[0]}" title="${escapeAttr(g.map(j => `${d.functions[j].letter}: ${d.functions[j].file_name || ''}`).join('\n'))}">${g.map(j => this._funcRef(j, 'xp-badge', false)).join('')}${g.length > 1 ? 'same code' : this._fileRef(g[0], 'xp-rfile', false)}</button>`).join('');
            let out = `<div class="xp-cbar"><span class="xp-hint" style="margin:0">Show</span>${chips}<label class="xp-chk"><input type="checkbox" data-xsingle ${single ? 'checked' : ''}> Function alone, in its own order</label></div>`;
            const tag = t => `<span class="xp-tag hot">${escapeHtml(t)}</span>`;
            if (single) {
                out += `<p class="xp-hint">Whole function ${this._fileRef(f, 'xp-rfile')}, blocks labelled with their match in the reference.</p>`;
                if (f === c.reference) return out + c.blocks.map(b => `<div class="xp-frow one">${this._band(b.id, 'same', 1, this._bt(b))}<pre class="xp-fcode">${this._codeHtml(b.ref_syntax, null, null, c.reference)}</pre></div>`).join('');
                const segs = c.funcs[String(f)] || [];
                return out + segs.map(seg => {
                    const r = seg.refs[0], b = r && c.blocks.find(x => x.id === r.b);
                    const band = r ? this._band(seg.refs.map(x => x.b).join(' + '), r.st, r.sim, this._bt(b)) : seg.x ? this._band(seg.x, 'extra', 1, (c.extras.find(x => x.id === seg.x) || {}).title || '') : this._band('-', 'extra', 1, 'unmatched');
                    return `<div class="xp-frow one">${band}<pre class="xp-fcode st-${r ? r.st : 'extra'}">${this._codeHtml(seg.syntax, seg.text, 'chg', f)}</pre></div>`;
                }).join('');
            }
            const ref = d.functions[c.reference];
            out += `<p class="xp-hint">Rows are blocks of ${this._funcRef(c.reference)}. Left is ${this._funcRef(c.reference)}, right is ${this._fileRef(f, 'xp-rfile')}. Functions sharing a block variant are grouped together; click a group to show its function throughout. Blocks that moved or merged stay on the row of the block they match.</p>`;
            out += `<div class="xp-frow xp-full-head"><div class="xp-fband">${this._funcRef(c.reference)} ${this._fileRef(c.reference, 'xp-rfile')} (reference)</div><div class="xp-fband">${this._funcRef(f)} ${this._fileRef(f, 'xp-rfile')}</div></div>`;
            const slots = this._extraSlots([f]);
            const extraRows = after => (slots.get(after) || []).map(({ seg, x }) =>
                `<div class="xp-frow">${this._band(seg.x, 'extra', 1, x ? x.title : '')}<pre class="xp-fcode gap">(no counterpart in ${this._funcRef(c.reference)})</pre><pre class="xp-fcode st-extra">${this._codeHtml(seg.syntax, seg.text, 'chg', f)}</pre></div>`
            ).join('');
            out += extraRows(null);
            let hidden = 0;
            for (const b of c.blocks) {
                const cell = this._cell(b, f);
                if (s.only && cell.st === 'same') hidden++;
                else {
                    const g = this._group(b, f);
                    let right;
                    if (cell.st === 'miss') right = '<pre class="xp-fcode gap">(no matching code)</pre>';
                    else if (cell.into) right = `<pre class="xp-fcode gap">↑ merged into ${escapeHtml(cell.into)} above</pre>`;
                    else right = `<pre class="xp-fcode st-${cell.st}" data-xfns="${f},${c.reference}">${this._codeHtml(this._cellSyntax(b, f), this._cellRuns(b, f), 'chg', f, this._hashSet(b.ref_syntax))}</pre>`;
                    out += `<div class="xp-frow">${this._band(b.id, cell.st, cell.sim, this._bt(b), cell.into ? tag('merged into ' + cell.into) : '', this._expl(b.id, f))}${this._fullGroups(b, f)}<pre class="xp-fcode" data-xfns="${c.reference},${f}">${this._codeHtml(b.ref_syntax, g && g.ref_segs, 'del', c.reference, this._hashSet(this._cellSyntax(b, f)))}</pre>${right}</div>`;
                }
                out += extraRows(b.id);
            }
            return out + (hidden ? `<p class="xp-foot">${hidden} block${hidden === 1 ? '' : 's'} identical to ${this._funcRef(c.reference)} hidden. Untick "Differences only" to read the whole function.</p>` : '');
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
            return `<p class="xp-hint">Pick any two of the ${n} functions. The existing 2-way diff is loaded here, so you do not leave the table.</p>
                <div class="xp-pairbar">Compare <select data-xsel="0">${opt(a)}</select> with <select data-xsel="1">${opt(b)}</select>
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
            const side = s => {
                if (!s) return '<code class="gap"></code>';
                const tag = ((s.chunk_class || '').match(/tag-(\w+)/) || [])[1] || 'equal';
                return `<code class="d-${tag}"><span class="no">${Number(s.line_idx)}</span>${(s.tokens || []).map(t => escapeHtml(t.text)).join('')}</code>`;
            };
            const rows = body.rows || [];
            host.innerHTML = `<div class="xp-drows">${rows.slice(0, PAIR_ROWS).map(r => `<div class="xp-drow">${side(r.l)}${side(r.r)}</div>`).join('')}</div>
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
            const fns = call.closest('pre[data-xfns]')?.dataset.xfns.split(',').map(Number);
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
            const box = call.closest('.xp-panes, .xp-frow');
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
            const t = e.target.closest('[data-xcall],[data-xfnref],[data-xfile],[data-xtab],[data-xcv],[data-xfn],[data-xg],[data-xc],[data-xx],[data-xb],[data-xexp],[data-xsave],[data-xask],[data-xpair],[data-xnav],[data-xopen]');
            if (!t) return;
            const d = t.dataset;
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
                const fns = t.closest('pre[data-xfns]')?.dataset.xfns.split(',').map(Number);
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
                if (d.xtab === 'code' && !this.data.consensus) {
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
            if (d.xcv) { this.s.cv = d.xcv; this._persist(); return this._rebody(); }
            if (d.xfn !== undefined) { this.s.fn = Number(d.xfn); this._persist(); return this._rebody(); }
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
            if (t.dataset.xonly !== undefined) { this.s.only = t.checked; this._persist(); return this._rebody(); }
            if (t.dataset.xsel !== undefined) {
                this.s.pair[Number(t.dataset.xsel)] = Number(t.value);
                this._loadPair();
            }
        }
    }

    window.NwayFunction = NwayFunction;
})();
