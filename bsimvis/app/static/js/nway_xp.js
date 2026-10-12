/**
 * Inline N-way function diff: the panel a row of the N-way table expands into
 * (GET /api/bin_sim/nway/facets). Header cards + similarity matrix, then
 * Facets / Consensus code / Pair diff tabs. The table is not re-rendered, so
 * its filters, scroll and focus stay put.
 *
 *   new NwayExpand(panel, tr, fids).open();   // panel = NwayPanel, tr = the row
 *
 * Every value comes off an uploaded sample: it all goes through escapeHtml /
 * escapeAttr.
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

    class NwayExpand {
        constructor(panel, tr, fids) {
            this.panel = panel;
            this.tr = tr;
            this.fids = fids;
            this.s = { tab: 'facets', only: true, pair: null, cv: 'blocks', open: null, gi: 0, fn: null, single: false };
            this.titles = {};
            this.data = null;
            this.row = null;
        }

        // Insert the expansion row under `tr` and fetch its data.
        async open() {
            const span = this.tr.children.length;
            this.row = document.createElement('tr');
            this.row.className = 'nway-xp-row';
            this.row.innerHTML = `<td colspan="${span}"><div class="nway-xp"><div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Comparing ${this.fids.length} functions...</div></div></td>`;
            this.tr.after(this.row);
            this.el = this.row.querySelector('.nway-xp');
            this.el.addEventListener('click', e => this._click(e));
            this.el.addEventListener('change', e => this._change(e));
            const scroller = this.row.closest('.nway-scroll');
            if (scroller && window.ResizeObserver) {
                this._ro = new ResizeObserver(() => { this.el.style.width = `${scroller.clientWidth - 2}px`; });
                this._ro.observe(scroller);
            }
            try {
                const qs = this.panel._docQs();
                qs.set('fids', this.fids.join(','));
                const res = await fetch(`/api/bin_sim/nway/facets?${qs.toString()}`);
                const body = await res.json().catch(() => ({}));
                if (!res.ok) throw new Error(body.message || body.error || body.detail || `HTTP ${res.status}`);
                this.data = body;
            } catch (e) {
                this.el.innerHTML = `<div class="nway-state">Could not compare these functions: ${escapeHtml(String(e.message || e))}</div>`;
                return;
            }
            this.render();
        }

        close() {
            if (this._ro) this._ro.disconnect();
            if (this.row) this.row.remove();
            this.row = null;
        }

        render() {
            const d = this.data, s = this.s;
            const code = d.consensus;
            const tab = (k, label, off, why) => `<button class="xp-tab" role="tab" data-xtab="${k}" aria-selected="${s.tab === k}"${off ? ` disabled title="${escapeAttr(why)}"` : ''}>${label}</button>`;
            this.el.innerHTML = `
                <div class="xp-head"><div class="xp-cols">${d.functions.map((f, j) => this._card(f, j)).join('')}</div>
                    <div><div class="xp-mx" style="--n:${d.functions.length}">${this._matrix()}</div>
                    <div class="xp-cap">Function similarity from the matcher; a dash means no match was found. Click a cell to open that pair in the 2-way diff.</div></div></div>
                <div class="xp-bar"><div class="xp-tabs" role="tablist">
                    ${tab('facets', 'Facets')}
                    ${tab('code', 'Code blocks', !code, `Code blocks need at most ${d.limits.code} functions (this row has ${d.functions.length})`)}
                    ${tab('pair', 'Pair diff')}</div><span class="xp-sp"></span>
                    ${s.tab === 'pair' ? '' : `<label class="xp-chk"><input type="checkbox" data-xonly ${s.only ? 'checked' : ''}> Differences only</label>`}
                    <button class="xp-btn" data-xclose title="Collapse this row"><i class="fa-solid fa-chevron-up"></i> Collapse</button></div>
                <div class="xp-body">${this._body()}</div>`;
            if (s.tab === 'pair') this._loadPair();
            if (s.tab === 'code') this._loadTitles();
        }

        _body() {
            const t = this.s.tab;
            return t === 'facets' ? this._facets() : t === 'code' ? this._code() : this._pair();
        }

        _rebody() {
            this.el.querySelector('.xp-body').innerHTML = this._body();
            if (this.s.tab === 'pair') this._loadPair();
        }

        _card(f, j) {
            const url = window.NwayPanel.fnUrl(f.id, this.panel.source.pool || null);
            const ref = j === this.data.reference ? '<span class="xp-ref" title="Reference of the consensus code: the function most similar to the others">ref</span>' : '';
            return `<div class="xp-col" ${cc(j)}>
                <div class="xp-l"><span class="xp-badge">${escapeHtml(f.letter)}</span><span class="xp-fn" title="${escapeAttr(f.file_name || '')}">${escapeHtml(f.file_name || '')}</span>${ref}</div>
                <div class="xp-f"><a data-xnav="${escapeAttr(url)}" data-title="${escapeAttr(f.name || f.addr)}" href="${escapeAttr(url)}">@ ${escapeHtml(f.addr)}</a>${f.instr != null ? ` &middot; ${Number(f.instr)} instr` : ''}</div>
                <div class="xp-t">${(f.tags || []).slice(0, 3).map(t => `<span class="xp-tag">${escapeHtml(t)}</span>`).join('')}</div></div>`;
        }

        _matrix() {
            const fs = this.data.functions, sim = this.data.similarity;
            let h = '<span></span>' + fs.map((f, j) => `<span class="xp-h" ${cc(j)}>${escapeHtml(f.letter)}</span>`).join('');
            sim.forEach((row, r) => {
                h += `<span class="xp-h" ${cc(r)}>${escapeHtml(fs[r].letter)}</span>`;
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
            return `<tr><th></th>${this.data.functions.map((f, j) => `<th ${cc(j)}>${escapeHtml(f.letter)}</th>`).join('')}</tr>`;
        }

        // The one place a string item links to the string pivot (functions sharing it):
        // opens the Strings dialog when that UI is loaded, else the item stays plain text.
        static stringPivot(item, collection, pool) {
            if (!window.StringsUI || !window.StringsUI.pivot) return false;
            window.StringsUI.pivot(collection, pool || '', item.key, item.label);
            return true;
        }

        _label(facet, item) {
            const text = escapeHtml(item.label);
            if (facet.key !== 'strings') return text;
            return window.StringsUI && window.StringsUI.pivot
                ? `<a href="#" data-xstr="${escapeAttr(item.key)}" title="Functions sharing this string">${text}</a>` : text;
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
            return `<div class="xp-grp">${head}<div class="xp-fx"><table>${this._thead()}${items.map(i => `<tr class="${i.count < n ? 'diff' : ''}"><td class="xp-i">${this._label(f, i)}<span class="xp-cnt">${Number(i.count)}/${n}</span></td>${this._dots(i.mask)}</tr>`).join('')}</table></div></div>`;
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

        _group(b, j) { return b.groups.find(g => g.funcs.includes(j)); }

        _glyph(st, sim, miss = true) {
            const G = NwayExpand.GLYPH;
            return `${G[st]}${st === 'same' || (st === 'miss' && miss) ? '' : ' ' + Math.round(sim * 100)}`;
        }

        _others() { return this.data.functions.map((f, j) => j).filter(j => j !== this.data.consensus.reference); }

        _isSame(b) { return Object.values(b.cells).every(c => c.st === 'same'); }

        _code() {
            const s = this.s;
            const sw = `<div class="xp-cbar"><div class="xp-seg" role="group" aria-label="Code view"><button data-xcv="blocks" aria-pressed="${s.cv === 'blocks'}">Blocks</button><button data-xcv="full" aria-pressed="${s.cv === 'full'}">Full function</button></div>
                <span class="xp-hint" style="margin:0">${s.cv === 'blocks' ? 'Overview of what changed, block by block.' : 'Read the whole function, aligned to the reference by block.'}</span>
                <span class="xp-naming" ${this._naming ? '' : 'hidden'}><i class="fa-solid fa-spinner fa-spin"></i> naming blocks&hellip;</span></div>`;
            return sw + (s.cv === 'blocks' ? this._blocksView() : this._fullView());
        }

        _ribbon(blocks, extras) {
            const d = this.data, c = d.consensus;
            let h = '<div class="xp-rib"><table><tr><th class="rowh">Block in ' + escapeHtml(d.functions[c.reference].letter) + '</th>'
                + blocks.map(b => `<th>${escapeHtml(b.id)}<small data-btitle="${escapeAttr(b.id)}" title="${escapeAttr(this._bt(b))}">${escapeHtml(this._bt(b))}</small></th>`).join('')
                + extras.map(x => `<th>${escapeHtml(x.id)}<small title="${escapeAttr(x.title)}">${escapeHtml(x.title)}</small></th>`).join('') + '</tr>';
            d.functions.forEach((f, j) => {
                h += `<tr><td class="rowh" ${cc(j)}><span class="xp-badge">${escapeHtml(f.letter)}</span> ${escapeHtml(f.file_name || '')}</td>`;
                for (const b of blocks) {
                    if (j === c.reference) { h += '<td><span class="st-none">ref</span></td>'; continue; }
                    const cell = this._cell(b, j);
                    const gsel = b.groups[this.s.gi];
                    const sel = this.s.open === b.id && !!gsel && gsel.funcs.includes(j);
                    h += `<td><button class="xp-cell st-${cell.st}${sel ? ' sel' : ''}" data-xb="${escapeAttr(b.id)}" data-xc="${j}" title="${NwayExpand.SNAME[cell.st]}">${this._glyph(cell.st, cell.sim)}</button></td>`;
                }
                for (const x of extras) h += x.funcs.includes(j) ? `<td><button class="xp-cell st-extra" data-xx="${escapeAttr(x.id)}" title="only in this function">+</button></td>` : '<td><span class="st-none">&ndash;</span></td>';
                h += '</tr>';
            });
            return h + '</table></div>';
        }

        _blockBody(b) {
            const d = this.data, c = d.consensus, ref = d.functions[c.reference];
            if (!b.groups.length) return `<div class="xp-bd"><p class="xp-hint" style="margin:0">Identical in all ${d.functions.length} functions.</p><div class="xp-panes"><div class="xp-pane"><div class="xp-ph"><span class="xp-badge" ${cc(c.reference)}>${escapeHtml(ref.letter)}</span> all functions</div><pre>${escapeHtml(b.ref_text)}</pre></div></div></div>`;
            const gi = b.groups[this.s.gi] ? this.s.gi : 0, g = b.groups[gi], G = NwayExpand.GLYPH;
            const badges = fs => fs.map(j => `<span class="xp-badge" ${cc(j)}>${escapeHtml(d.functions[j].letter)}</span>`).join('');
            const chips = b.groups.map((x, i) => `<button class="xp-chip" aria-pressed="${i === gi}" data-xb="${escapeAttr(b.id)}" data-xg="${i}">${badges(x.funcs)} ${G[x.st]} ${NwayExpand.SNAME[x.st]}</button>`).join('');
            const same = this._others().filter(j => this._cell(b, j).st === 'same');
            const j0 = g.funcs[0], cell = this._cell(b, j0);
            const runs = this._cellRuns(b, j0);
            const sim = g.funcs.map(j => Math.round(g.sims[String(j)] * 100)).join('/');
            return `<div class="xp-bd"><div class="xp-vchips"><span class="lab">Compare ${escapeHtml(ref.letter)} with</span>${chips}${same.length ? `<span class="lab">${same.map(j => escapeHtml(d.functions[j].letter)).join(', ')}: same as ${escapeHtml(ref.letter)}</span>` : ''}</div>
                <div class="xp-panes">
                  <div class="xp-pane"><div class="xp-ph"><span class="xp-badge" ${cc(c.reference)}>${escapeHtml(ref.letter)}</span> reference</div><pre>${g.ref_segs ? this._runs(g.ref_segs, 'del') : escapeHtml(b.ref_text)}</pre></div>
                  <div class="xp-pane"><div class="xp-ph">${badges(g.funcs)} <span class="xp-cell st-${g.st}" style="cursor:default">${G[g.st]} ${NwayExpand.SNAME[g.st]}${g.st === 'miss' ? '' : ' ' + sim + '%'}</span>${cell.into ? `<span class="xp-tag hot">merged into ${escapeHtml(cell.into)}</span>` : ''}</div>
                    <pre>${runs.length ? this._runs(runs, 'chg') : '(no matching code)'}</pre>${g.note ? `<div class="xp-pnote">${escapeHtml(g.note)}</div>` : ''}</div>
                </div></div>`;
        }

        _blocksView() {
            const c = this.data.consensus, only = this.s.only;
            const blocks = c.blocks.filter(b => !only || !this._isSame(b));
            const G = NwayExpand.GLYPH;
            const head = `<p class="xp-hint">When decompiled code is laid out differently, lines do not match one to one. Functions are cut into blocks (loops, branches, straight runs) and each block is matched by its content, in any order. Each cell shows how that block looks in the other function: how similar it is, or that it moved, was merged into or split from another block, or is gone. Click a cell or a block to compare it with the reference.</p>`;
            const legend = `<div class="xp-legend">${Object.keys(NwayExpand.SNAME).map(k => `<span><span class="xp-cell st-${k}" style="min-width:0;height:18px;cursor:default">${G[k]}</span>${NwayExpand.SNAME[k]}</span>`).join('')}</div>`;
            const accordion = blocks.map(b => {
                const open = this.s.open === b.id;
                const mini = this._others().map(j => { const x = this._cell(b, j); return `<span class="st-${x.st}" ${cc(j)} title="${escapeAttr(`${this.data.functions[j].letter}: ${NwayExpand.SNAME[x.st]}`)}">${escapeHtml(this.data.functions[j].letter)}${G[x.st]}</span>`; }).join('');
                return `<div class="xp-blk"><button class="xp-bh" data-xb="${escapeAttr(b.id)}" aria-expanded="${open}"><span class="xp-bid">${escapeHtml(b.id)}</span>${this._tspan(b)}<span class="xp-mini">${mini}</span></button>${open ? this._blockBody(b) : ''}</div>`;
            }).join('');
            const extras = c.extras.length ? '<div class="xp-sec">Blocks not in ' + escapeHtml(this.data.functions[c.reference].letter) + '</div>' + c.extras.map(x => {
                const open = this.s.open === x.id;
                return `<div class="xp-blk"><button class="xp-bh" data-xb="${escapeAttr(x.id)}" aria-expanded="${open}"><span class="xp-bid">${escapeHtml(x.id)}</span><span class="bt">${escapeHtml(x.title)}</span><span class="xp-mini">${x.funcs.map(j => `<span class="st-extra">${escapeHtml(this.data.functions[j].letter)}+</span>`).join('')}</span></button>${open ? `<div class="xp-bd"><div class="xp-panes"><div class="xp-pane"><div class="xp-ph">${x.funcs.map(j => `<span class="xp-badge" ${cc(j)}>${escapeHtml(this.data.functions[j].letter)}</span>`).join('')} <span class="xp-tag">no counterpart in the reference</span></div><pre>${escapeHtml(x.text)}</pre></div></div></div>` : ''}</div>`;
            }).join('') : '';
            const hidden = only ? c.blocks.length - blocks.length : 0;
            return head + this._ribbon(blocks, c.extras) + legend + accordion + extras
                + (hidden ? `<p class="xp-foot">${hidden} block${hidden === 1 ? '' : 's'} identical in all ${this.data.functions.length} functions hidden.</p>` : '')
                + (c.truncated ? `<p class="xp-foot">Large functions are compared on their first ${Number(this.data.limits.lines)} lines and ${Number(this.data.limits.blocks)} blocks.</p>` : '');
        }

        _band(id, st, sim, title, extra = '') {
            return `<div class="xp-fband"><span class="xp-bid">${escapeHtml(id)}</span><span class="xp-cell st-${st}" style="cursor:default">${NwayExpand.GLYPH[st]} ${NwayExpand.SNAME[st]}${st === 'miss' || st === 'same' || st === 'extra' ? '' : ' ' + Math.round(sim * 100) + '%'}</span>${extra}<span class="dim">${escapeHtml(title)}</span><i></i></div>`;
        }

        _fullView() {
            const d = this.data, c = d.consensus, s = this.s, single = s.single;
            if (s.fn == null || (!single && s.fn === c.reference)) s.fn = this._others()[0];
            const f = s.fn;
            const chips = d.functions.map((fn, j) => (j === c.reference && !single) ? '' : `<button class="xp-chip" aria-pressed="${j === f}" data-xfn="${j}"><span class="xp-badge" ${cc(j)}>${escapeHtml(fn.letter)}</span>${escapeHtml(fn.file_name || '')}</button>`).join('');
            let out = `<div class="xp-cbar"><span class="xp-hint" style="margin:0">Show</span>${chips}<label class="xp-chk"><input type="checkbox" data-xsingle ${single ? 'checked' : ''}> Function alone, in its own order</label></div>`;
            const tag = t => `<span class="xp-tag hot">${escapeHtml(t)}</span>`;
            if (single) {
                out += `<p class="xp-hint">Whole function ${escapeHtml(d.functions[f].file_name || '')}, blocks labelled with their match in the reference.</p>`;
                if (f === c.reference) return out + c.blocks.map(b => `<div class="xp-frow one">${this._band(b.id, 'same', 1, this._bt(b))}<pre class="xp-fcode">${escapeHtml(b.ref_text)}</pre></div>`).join('');
                const segs = c.funcs[String(f)] || [];
                return out + segs.map(seg => {
                    const r = seg.refs[0], b = r && c.blocks.find(x => x.id === r.b);
                    const band = r ? this._band(seg.refs.map(x => x.b).join(' + '), r.st, r.sim, this._bt(b)) : seg.x ? this._band(seg.x, 'extra', 1, (c.extras.find(x => x.id === seg.x) || {}).title || '') : this._band('-', 'extra', 1, 'unmatched');
                    return `<div class="xp-frow one">${band}<pre class="xp-fcode st-${r ? r.st : 'extra'}">${this._runs(seg.text, 'chg')}</pre></div>`;
                }).join('');
            }
            const ref = d.functions[c.reference];
            out += `<p class="xp-hint">Rows are blocks of ${escapeHtml(ref.letter)}. Left is ${escapeHtml(ref.letter)}, right is ${escapeHtml(d.functions[f].file_name || '')}. Blocks that moved or merged stay on the row of the block they match.</p>`;
            out += `<div class="xp-frow"><div class="xp-fband" style="grid-column:1"><span class="xp-badge" ${cc(c.reference)}>${escapeHtml(ref.letter)}</span> ${escapeHtml(ref.file_name || '')} (reference)</div><div class="xp-fband" style="grid-column:2"><span class="xp-badge" ${cc(f)}>${escapeHtml(d.functions[f].letter)}</span> ${escapeHtml(d.functions[f].file_name || '')}</div></div>`;
            let hidden = 0;
            for (const b of c.blocks) {
                const cell = this._cell(b, f);
                if (s.only && cell.st === 'same') { hidden++; continue; }
                const g = this._group(b, f);
                let right;
                if (cell.st === 'miss') right = '<pre class="xp-fcode gap">(no matching code)</pre>';
                else if (cell.into) right = `<pre class="xp-fcode gap">↑ merged into ${escapeHtml(cell.into)} above</pre>`;
                else right = `<pre class="xp-fcode st-${cell.st}">${this._runs(this._cellRuns(b, f), 'chg')}</pre>`;
                out += `<div class="xp-frow">${this._band(b.id, cell.st, cell.sim, this._bt(b), cell.into ? tag('merged into ' + cell.into) : '')}<pre class="xp-fcode">${g && g.ref_segs ? this._runs(g.ref_segs, 'del') : escapeHtml(b.ref_text)}</pre>${right}</div>`;
            }
            for (const seg of (c.funcs[String(f)] || []).filter(x => x.x)) {
                const x = c.extras.find(e => e.id === seg.x);
                out += `<div class="xp-frow">${this._band(seg.x, 'extra', 1, x ? x.title : '')}<pre class="xp-fcode gap">(no counterpart in ${escapeHtml(ref.letter)})</pre><pre class="xp-fcode st-extra">${this._runs(seg.text, 'chg')}</pre></div>`;
            }
            return out + (hidden ? `<p class="xp-foot">${hidden} block${hidden === 1 ? '' : 's'} identical to ${escapeHtml(ref.letter)} hidden. Untick "Differences only" to read the whole function.</p>` : '');
        }

        // LLM titles: asked once when the code tab first opens, never blocking the render.
        async _loadTitles() {
            if (this._titlesAsked) return;
            this._titlesAsked = true;
            const qs = this.panel._docQs();
            qs.set('fids', this.fids.join(','));
            for (let i = 0; i < 40 && this.row; i++) {
                let body = null;
                try {
                    const res = await fetch(`/api/bin_sim/nway/facets/titles?${qs.toString()}`);
                    body = res.ok ? await res.json() : null;
                } catch (e) { body = null; }
                if (!this.row) return;
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
                <span class="xp-tag" id="xp-pair-sim"></span><button class="xp-btn" data-xopen>Open full diff</button></div>
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
            if (this.panel.source.pool) qs.set('pool', this.panel.source.pool);
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
        _openDiff(a, b, e) {
            const fs = this.data.functions;
            window.Nav.openPath(window.buildDiffUrl(this.fids[a], this.fids[b]), e, { title: `Diff: ${fs[a].addr} vs ${fs[b].addr}`, type: 'diff' });
        }

        _click(e) {
            const t = e.target.closest('[data-xstr],[data-xtab],[data-xcv],[data-xfn],[data-xg],[data-xc],[data-xx],[data-xb],[data-xpair],[data-xnav],[data-xclose],[data-xopen]');
            if (!t) return;
            const d = t.dataset;
            if (d.xclose !== undefined) {
                const btn = this.tr.querySelector('[data-xp]');
                this.panel._xpClose(this.tr);
                if (btn) btn.focus();
                return;
            }
            if (d.xstr) {
                e.preventDefault();
                const f = this.data.facets.find(x => x.key === 'strings');
                const item = f && f.items.find(i => i.key === d.xstr);
                return item && NwayExpand.stringPivot(item, this.fids[0].split(':func:')[0], this.panel.source.pool);
            }
            if (d.xtab) { this.s.tab = d.xtab; return this.render(); }
            if (d.xpair) { const [a, b] = d.xpair.split(',').map(Number); return this._openDiff(a, b, e); }
            if (d.xopen !== undefined) return this._openDiff(...this.s.pair, e);
            if (d.xnav) {
                e.preventDefault();
                return window.Nav.openPath(d.xnav, e, { title: d.title || 'Function', type: 'function' });
            }
            if (d.xcv) { this.s.cv = d.xcv; return this._rebody(); }
            if (d.xfn !== undefined) { this.s.fn = Number(d.xfn); return this._rebody(); }
            if (d.xx) { this.s.open = d.xx; return this._rebody(); }
            if (d.xb !== undefined) {
                const b = this.data.consensus.blocks.find(x => x.id === d.xb);
                if (d.xg !== undefined) { this.s.open = d.xb; this.s.gi = Number(d.xg); }
                else if (d.xc !== undefined && b) {
                    this.s.open = d.xb;
                    this.s.gi = Math.max(0, b.groups.findIndex(g => g.funcs.includes(Number(d.xc))));
                } else { this.s.open = this.s.open === d.xb ? null : d.xb; this.s.gi = 0; }
                this._rebody();
            }
        }

        _change(e) {
            const t = e.target;
            if (t.dataset.xsingle !== undefined) { this.s.single = t.checked; return this._rebody(); }
            if (t.dataset.xonly !== undefined) { this.s.only = t.checked; return this._rebody(); }
            if (t.dataset.xsel !== undefined) {
                this.s.pair[Number(t.dataset.xsel)] = Number(t.value);
                this._loadPair();
            }
        }
    }

    window.NwayExpand = NwayExpand;
})();
