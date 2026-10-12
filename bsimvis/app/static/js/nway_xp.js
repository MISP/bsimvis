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
            this.s = { tab: 'facets', only: true, pair: null };
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
            this.el.addEventListener('keydown', e => {
                if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('.xp-ln')) { e.preventDefault(); e.target.click(); }
            });
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
                    ${tab('code', 'Consensus code', !code, `Consensus code needs at most ${d.limits.code} functions (this row has ${d.functions.length})`)}
                    ${tab('pair', 'Pair diff')}</div><span class="xp-sp"></span>
                    ${s.tab === 'pair' ? '' : `<label class="xp-chk"><input type="checkbox" data-xonly ${s.only ? 'checked' : ''}> Differences only</label>`}
                    <button class="xp-btn" data-xclose title="Collapse this row"><i class="fa-solid fa-chevron-up"></i> Collapse</button></div>
                <div class="xp-body">${this._body()}</div>`;
            if (s.tab === 'pair') this._loadPair();
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

        // The one place a string item can link to a string pivot view: return a URL (or null).
        static stringLink(item) { return null; }

        _label(facet, item) {
            const text = escapeHtml(item.label);
            if (facet.key !== 'strings') return text;
            const url = NwayExpand.stringLink(item);
            return url ? `<a data-xnav="${escapeAttr(url)}" href="${escapeAttr(url)}">${text}</a>` : text;
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

        // ---- Consensus code ----
        _strip(mask) {
            return [...mask].map((x, j) => `<i ${cc(j)} class="${x === '1' ? '' : 'off'}"></i>`).join('');
        }

        _code() {
            const c = this.data.consensus, n = this.data.functions.length, only = this.s.only;
            const ref = this.data.functions[c.reference].letter;
            let hidden = 0;
            const lines = c.lines.map((l, i) => {
                const all = !l.ins && !l.mask.includes('0');
                if (only && all) { hidden++; return ''; }
                const cnt = [...l.mask].filter(x => x === '1').length;
                return `<div class="xp-ln${l.ins ? ' ins' : all ? '' : ' var'}" data-xline="${i}" tabindex="0" role="button" aria-expanded="false">
                    <span class="no">${l.ins ? '+' : Number(l.n)}</span><span class="cs">${this._strip(l.mask)}</span><span class="ct">${cnt}/${n}</span><span class="tx">${escapeHtml(l.text)}</span></div>`;
            }).join('');
            return `<p class="xp-hint">Reference is ${escapeHtml(ref)}, the function most similar to the others. The bar next to each line shows which functions have it unchanged. Click a line to see the other versions. Dashed lines are not in ${escapeHtml(ref)}.</p>
                <div class="xp-code">${lines || '<div class="xp-none">No differing lines.</div>'}</div>
                ${hidden ? `<p class="xp-foot">${hidden} lines identical in all ${n} are hidden. Untick "Differences only" to see the full function.</p>` : ''}
                ${c.truncated ? `<p class="xp-foot">Only the first ${Number(this.data.limits.lines)} lines of each function are compared.</p>` : ''}
                ${c.extras_dropped ? `<p class="xp-foot">${Number(c.extras_dropped)} more lines that exist only in other functions are not listed.</p>` : ''}`;
        }

        _variants(l) {
            const c = this.data.consensus, fs = this.data.functions;
            const rows = fs.map((f, j) => {
                const has = l.mask[j] === '1';
                let body;
                if (j === c.reference) body = l.ins ? '<span class="gone">not in the reference</span>' : '<span class="same">reference</span>';
                else if (l.ins) body = has ? `<span class="chg"><b>${escapeHtml(l.text.trim())}</b></span>` : '<span class="gone">not present</span>';
                else if (has) body = `<span class="same">same as ${escapeHtml(fs[c.reference].letter)}</span>`;
                else {
                    const v = l.v && l.v[j];
                    body = v ? `<span class="chg">${v.map(([t, ch], k) => (ch ? `<b>${escapeHtml(k ? t : t.trimStart())}</b>` : escapeHtml(k ? t : t.trimStart()))).join('')}</span>` : '<span class="gone">line removed</span>';
                }
                return `<div><span class="xp-badge" ${cc(j)}>${escapeHtml(f.letter)}</span>${body}</div>`;
            });
            return `<div class="xp-vr">${rows.join('')}</div>`;
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
            const t = e.target.closest('[data-xtab],[data-xline],[data-xpair],[data-xnav],[data-xclose],[data-xopen]');
            if (!t) return;
            const d = t.dataset;
            if (d.xclose !== undefined) {
                const btn = this.tr.querySelector('[data-xp]');
                this.panel._xpClose(this.tr);
                if (btn) btn.focus();
                return;
            }
            if (d.xtab) { this.s.tab = d.xtab; return this.render(); }
            if (d.xpair) { const [a, b] = d.xpair.split(',').map(Number); return this._openDiff(a, b, e); }
            if (d.xopen !== undefined) return this._openDiff(...this.s.pair, e);
            if (d.xnav) {
                e.preventDefault();
                return window.Nav.openPath(d.xnav, e, { title: d.title || 'Function', type: 'function' });
            }
            if (d.xline !== undefined) {
                const next = t.nextElementSibling;
                const open = next && next.classList.contains('xp-vr');
                if (open) next.remove(); else t.insertAdjacentHTML('afterend', this._variants(this.data.consensus.lines[Number(d.xline)]));
                t.classList.toggle('open', !open);
                t.setAttribute('aria-expanded', String(!open));
            }
        }

        _change(e) {
            const t = e.target;
            if (t.dataset.xonly !== undefined) { this.s.only = t.checked; return this._rebody(); }
            if (t.dataset.xsel !== undefined) {
                this.s.pair[Number(t.dataset.xsel)] = Number(t.value);
                this._loadPair();
            }
        }
    }

    window.NwayExpand = NwayExpand;
})();
