/**
 * Overview tab of a file cluster: one card per tab (Members, Metadata,
 * Functions) saying what is behind it, then the presence matrix (shared lanes
 * x member files) and the member fingerprints (each file's lanes in fixed
 * slots, so a gap is a lane the file lacks).
 *
 *   ClusterOverview.render(el, ClusterDetailView);
 *
 * The Members and Metadata cards draw from what the view already loaded. The
 * lanes come from one /api/bin_sim/nway call (`columns=files`, lanes closed, no
 * page limit: the groups without their rows), drawn when it answers. File and
 * function names come off uploaded samples, so every value is escaped.
 */
(function () {
    const MAX_LANES = 15;
    const TOP = 5;
    const LANE_TIER = 0.8; // the Functions tab's default lane tier
    const COLOR = { core: 'var(--success)', common: 'var(--accent)', rare: 'var(--info)', uniq: 'var(--warning, #fd971f)', partial: 'var(--accent)' };

    const CSS = `
.cov { display:flex; flex-direction:column; gap:14px; padding-bottom:20px; }
.cov-hub { display:grid; grid-template-columns:repeat(auto-fit, minmax(260px, 1fr)); gap:12px; }
.cov-card { background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:14px 16px; display:flex; flex-direction:column; gap:10px; min-width:0; }
.cov-head { display:flex; align-items:baseline; justify-content:space-between; gap:8px; }
.cov-head b { font-size:0.95rem; }
.cov-q { font-size:0.75rem; color:var(--dim); }
.cov-foot { margin-top:auto; display:flex; gap:6px; flex-wrap:wrap; }
.cov-h3 { font-size:0.62rem; text-transform:uppercase; letter-spacing:1px; color:var(--dim); margin-bottom:4px; }
.cov-list { display:flex; flex-direction:column; gap:3px; font-size:0.78rem; }
.cov-row { display:grid; grid-template-columns:14px minmax(0,1fr) 60px 40px; gap:6px; align-items:center; min-width:0; }
.cov-row > * { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cov-meter { height:6px; background:var(--hover); border-radius:3px; overflow:hidden; }
.cov-meter > span { display:block; height:100%; background:var(--accent); }
.cov-sbar { display:flex; gap:1px; height:12px; border-radius:3px; overflow:hidden; background:var(--hover); }
.cov-sbar > span { display:block; min-width:2px; font-size:0.62rem; line-height:14px; color:var(--bg); padding-left:4px; white-space:nowrap; overflow:hidden; }
.cov-sbar.tall { height:14px; }
.cov-dist { display:grid; grid-template-columns:70px minmax(0,1fr); gap:8px; align-items:center; font-size:0.75rem; }
.cov-legend { display:flex; gap:12px; flex-wrap:wrap; font-size:0.72rem; color:var(--dim); }
.cov-legend i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:-1px; }
.cov-legend b { color:var(--text); }
.cov-chips { display:flex; gap:4px; flex-wrap:wrap; }
.cov-chip { font-size:0.68rem; padding:0 6px; border-radius:4px; border:1px solid var(--border); white-space:nowrap; max-width:100%; overflow:hidden; text-overflow:ellipsis; }
.cov-lane { display:grid; grid-template-columns:minmax(0,1fr) auto 40px; gap:8px; align-items:center; font-size:0.76rem; padding:2px 4px; border-radius:4px; cursor:pointer; }
.cov-lane:hover { background:var(--hover); }
.cov-lane > span:first-child { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cov-dots { display:flex; gap:2px; }
.cov-dots i { width:5px; height:10px; border-radius:1px; background:var(--hover); display:block; }
.cov-panel { background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:14px 16px; display:flex; flex-direction:column; gap:10px; min-width:0; }
.cov-panel-head { display:flex; justify-content:space-between; gap:8px; flex-wrap:wrap; align-items:baseline; }
.cov-scroll { overflow-x:auto; }
.cov-mx { display:grid; align-items:stretch; font-size:0.76rem; width:max-content; }
.cov-mx > div { min-width:0; }
.cov-mx .bar { display:flex; flex-direction:column; justify-content:flex-end; align-items:center; gap:2px; height:90px; cursor:pointer; font-size:0.66rem; }
.cov-mx .bar i { display:block; width:16px; border-radius:2px 2px 0 0; }
.cov-mx .name { display:flex; align-items:center; gap:4px; padding-right:8px; height:22px; overflow:hidden; white-space:nowrap; }
.cov-mx .tot { display:flex; align-items:center; gap:4px; justify-content:flex-end; padding-right:8px; height:22px; color:var(--dim); font-size:0.68rem; }
.cov-mx .tot i { display:block; height:8px; border-radius:2px; background:var(--dim); opacity:.6; }
.cov-mx .cell { position:relative; height:22px; display:flex; align-items:center; justify-content:center; cursor:pointer; }
.cov-mx .cell::before { content:''; position:absolute; left:50%; width:2px; margin-left:-1px; background:var(--ln); }
.cov-mx .cell.full::before { top:0; bottom:0; }
.cov-mx .cell.lo::before { top:50%; bottom:0; }
.cov-mx .cell.hi::before { top:0; bottom:50%; }
.cov-mx .cell b { position:relative; width:10px; height:10px; border-radius:50%; background:var(--ln); }
.cov-mx .cell s { position:relative; width:6px; height:6px; border-radius:50%; background:var(--border); }
.cov-mx .lbl { writing-mode:vertical-rl; transform:rotate(180deg); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-height:150px; padding:6px 0 0; color:var(--dim); font-size:0.68rem; justify-self:center; cursor:pointer; }
.cov-mx .stripe { background:color-mix(in srgb, var(--hover) 55%, transparent); }
.cov-fp { display:flex; flex-direction:column; gap:3px; min-width:560px; }
.cov-fp .r { display:grid; grid-template-columns:200px minmax(0,1fr) 48px; gap:8px; align-items:center; font-size:0.78rem; }
.cov-fp .r > span:first-child { overflow:hidden; white-space:nowrap; display:flex; gap:4px; align-items:center; }
.cov-fp .hdr { color:var(--dim); font-size:0.62rem; text-transform:uppercase; letter-spacing:1px; }
.cov-track { display:flex; gap:2px; height:16px; }
.cov-track > span { height:100%; border-radius:2px; min-width:3px; }
.cov-track > span.miss { background:repeating-linear-gradient(135deg, transparent 0 3px, color-mix(in srgb, var(--dim) 35%, transparent) 3px 4px); border:1px dashed color-mix(in srgb, var(--dim) 60%, transparent); }
.cov-track > span.go { cursor:pointer; }
.cov-lanehdr { display:flex; gap:2px; }
.cov-lanehdr > span { min-width:3px; border-top:2px solid; padding-top:2px; overflow:hidden; white-space:nowrap; text-overflow:clip; font-size:0.6rem; }
.cov-wait { color:var(--dim); font-size:0.78rem; padding:6px 0; }
`;

    function css() {
        if (document.getElementById('cov-css')) return;
        const s = document.createElement('style');
        s.id = 'cov-css';
        s.textContent = CSS;
        document.head.appendChild(s);
    }

    const num = v => Number(v || 0).toLocaleString();
    const ones = mask => mask.split('1').length - 1;
    const share = (v, total) => `${total ? (v / total * 100).toFixed(2) : 0}%`;
    const spin = text => `<div class="cov-wait"><i class="fa-solid fa-spinner fa-spin"></i> ${escapeHtml(text)}</div>`;

    // Same bands as the server's lane tiers (nway_group_service._lane_tiers).
    function tierOf(files, n) {
        const core = Math.max(2, Math.ceil(LANE_TIER * n));
        const common = Math.max(2, Math.ceil(core / 2));
        return files >= core ? 'core' : files >= common ? 'common' : 'rare';
    }

    // [color, value, label] parts of one stacked bar.
    const sbar = (parts, tall) => `<div class="cov-sbar${tall ? ' tall' : ''}">${parts.filter(p => p[1] > 0)
        .map(([color, v, label]) => `<span style="flex:${Number(v)}; background:${color}" title="${escapeAttr(`${label}: ${num(v)}`)}">${tall ? escapeHtml(label) : ''}</span>`).join('')}</div>`;

    const button = (label, onClick, primary) => `<button class="btn-code-action" style="${primary ? 'border-color:var(--accent); color:var(--accent);' : ''}" onclick="${escapeAttr(onClick)}">${escapeHtml(label)}</button>`;

    function membersCard(v, c) {
        const direct = v.memberCache[c.cluster_uuid] || [];
        const kids = (v.childrenMap[c.cluster_id] || []).map(id => v.clusterMapById[id]).filter(Boolean);
        let body;
        if (direct.length) {
            const top = [...direct].sort((a, b) => (b.centrality ?? -1) - (a.centrality ?? -1)).slice(0, TOP);
            body = top.map(m => {
                const id = String(m.id || '');
                const col = id.includes(':') ? id.split(':')[0] : v.collection || '';
                const cen = m.centrality == null ? null : Number(m.centrality);
                return `<div class="cov-row"><span style="color:var(--accent)">${c.medoid && id === c.medoid ? '★' : ''}</span>
                    ${EntityRenderer.renderFileName(m.name || '', m.file_md5 || '', col)}
                    <span class="cov-meter"><span style="width:${cen == null ? 0 : cen * 100}%"></span></span>
                    <span class="dim" style="text-align:right">${cen == null ? '---' : cen.toFixed(2)}</span></div>`;
            }).join('') + (direct.length > TOP ? `<div class="dim" style="font-size:0.72rem; padding-left:20px;">+${num(direct.length - TOP)} more</div>` : '');
        } else if (kids.length) {
            body = kids.slice(0, TOP).map(k => `<div class="cov-row" style="cursor:pointer" onclick="ClusterDetailView.selectNode(${escapeAttr(jsString(k.cluster_uuid))})">
                <i class="fa-solid fa-bullseye" style="color:var(--accent)"></i><span>${escapeHtml(k.cluster_name || `Cluster #${k.cluster_id}`)}</span>
                <span></span><span class="dim" style="text-align:right">${num(k.count)}</span></div>`).join('');
        } else {
            body = `<div class="dim">No members listed.</div>`;
        }
        return `<div class="cov-card">
            <div class="cov-head"><b>Members</b><span class="dim">${num(c.count)} files${kids.length ? ` · ${num(kids.length)} sub-clusters` : ''}</span></div>
            <div class="cov-q">${direct.length ? 'Who is in it, most central first' : 'Its biggest sub-clusters'}</div>
            <div class="cov-list">${body}</div>
            <div class="cov-foot">${button(`All ${num(c.count)} members →`, "ClusterDetailView.switchTab('members')", true)}</div>
        </div>`;
    }

    function metadataCard(c) {
        const dists = [['Arch', c.architecture_distribution], ['Format', c.executable_format_distribution], ['Batch', c.batch_uuid_distribution]]
            .filter(([, d]) => d && d.length);
        const rows = dists.map(([label, d]) => `<div class="cov-dist"><span class="dim">${label}</span>${sbar(d.map((it, i) => [metadataColor(it, i), it.count, String(it.value ?? '')]), true)}</div>`).join('');
        const tags = [c.tag_distribution, c.file_tag_distribution].flatMap(by => Object.values(by || {}).flat())
            .sort((a, b) => metadataPercent(b) - metadataPercent(a)).slice(0, TOP);
        const s = c.function_count_stats || {};
        const fpm = s.min === undefined ? '' : `<div><div class="cov-h3">Functions per member</div>
            <div style="display:flex; gap:18px; font-size:0.8rem;">${[['min', s.min], ['avg', s.avg], ['max', s.max]].map(([l, x]) => `<span><span class="dim">${l}</span> <b>${num(Math.round(x))}</b></span>`).join('')}</div></div>`;
        return `<div class="cov-card">
            <div class="cov-head"><b>Metadata</b><span class="dim">${num(c.count)} files · ${num((c.architecture_distribution || []).length)} arch</span></div>
            <div class="cov-q">What the files have in common</div>
            ${rows ? `<div style="display:flex; flex-direction:column; gap:6px;">${rows}</div>` : `<div class="dim" style="font-size:0.78rem;">No distributions stored for this cluster. A cluster below the configured min_cohesion reports none.</div>`}
            ${fpm}
            ${tags.length ? `<div class="cov-chips">${tags.map(t => `<span class="cov-chip" title="${escapeAttr(t.tag_id)}">${escapeHtml(t.tag_id)} <span class="dim">${metadataPercent(t).toFixed(0)}%</span></span>`).join('')}</div>` : ''}
            <div class="cov-foot">${button('All distributions →', "ClusterDetailView.switchTab('metadata')", true)}</div>
        </div>`;
    }

    function functionsCard(c) {
        return `<div class="cov-card">
            <div class="cov-head"><b>Functions</b><span class="dim" id="cov-fn-total"></span></div>
            <div class="cov-q">What code they share</div>
            <div id="cov-fn-body">${spin(`Aligning functions across ${num(c.count)} files…`)}</div>
            <div class="cov-foot">${button('N-way table →', "ClusterOverview.openFunctions('all')", true)}<span id="cov-fn-uniq"></span></div>
        </div>`;
    }

    const laneName = (names, g) => (names[g.id] && !/^unnamed/i.test(names[g.id])) ? names[g.id] : g.hint ? `${g.hint}…` : `${ones(g.mask)}-file lane`;
    const laneTip = (g, n) => `${g.rows} functions in ${ones(g.mask)} of ${n} files`;

    function fnCardBody(m) {
        const k = m.counts;
        const parts = [[COLOR.core, k.core, 'Core'], [COLOR.partial, k.partial, 'Partial'], [COLOR.uniq, k.unique, 'Unique']];
        const dots = g => `<span class="cov-dots">${[...g.mask].map(b => `<i style="${b === '1' ? `background:${COLOR[g.tier]}` : ''}"></i>`).join('')}</span>`;
        const top = [...m.lanes].sort((a, b) => b.rows - a.rows).slice(0, TOP);
        return `<div style="display:flex; flex-direction:column; gap:8px;">${sbar(parts, true)}
            <div class="cov-legend">${parts.map(([c, v, l]) => `<span><i style="background:${c}"></i>${l} <b>${num(v)}</b></span>`).join('')}</div>
            ${top.length ? `<div><div class="cov-h3">Biggest shared lanes</div>${top.map(g => `<div class="cov-lane" title="${escapeAttr(laneTip(g, m.n))}" onclick="ClusterOverview.openFunctions('all')">
                <span data-cov-lname="${escapeAttr(g.id)}">${escapeHtml(laneName(m.names, g))}</span>${m.n <= 40 ? dots(g) : `<span class="dim">${ones(g.mask)}/${m.n}</span>`}<span class="dim" style="text-align:right">${num(g.rows)}</span></div>`).join('')}</div>` : ''}</div>`;
    }

    function matrix(m, medoid) {
        const { n, cols, lanes, names } = m;
        const maxRows = Math.max(1, ...lanes.map(g => g.rows));
        const maxTot = Math.max(1, ...m.totals);
        const span = lanes.map(g => [g.mask.indexOf('1'), g.mask.lastIndexOf('1')]);
        let h = `<div></div><div class="tot" style="align-items:flex-end; height:90px;">functions</div>`;
        h += lanes.map(g => `<div class="bar" title="${escapeAttr(laneTip(g, n))}" onclick="ClusterOverview.openFunctions('all')"><span>${num(g.rows)}</span><i style="height:${Math.max(2, g.rows / maxRows * 64)}px; background:${COLOR[g.tier]}"></i></div>`).join('');
        cols.forEach((col, i) => {
            const stripe = i % 2 ? '' : ' stripe';
            h += `<div class="name${stripe}">${col.id === medoid ? '<span style="color:var(--accent)" title="Main file (medoid)">★</span>' : ''}${EntityRenderer.renderFileName(col.label, col.md5, col.coll)}</div>`;
            h += `<div class="tot${stripe}"><i style="width:${m.totals[i] / maxTot * 36}px"></i>${num(m.totals[i])}</div>`;
            h += lanes.map((g, j) => {
                const [a, b] = span[j];
                const line = a === b ? '' : i === a ? ' lo' : i === b ? ' hi' : i > a && i < b ? ' full' : '';
                return `<div class="cell${stripe}${line}" style="--ln:${COLOR[g.tier]}" onclick="ClusterOverview.openFunctions('all')">${g.mask[i] === '1' ? '<b></b>' : '<s></s>'}</div>`;
            }).join('');
        });
        h += `<div></div><div></div>` + lanes.map(g => `<div class="lbl" title="${escapeAttr(laneName(names, g))}" data-cov-lname="${escapeAttr(g.id)}" onclick="ClusterOverview.openFunctions('all')">${escapeHtml(laneName(names, g))}</div>`).join('');
        return `<div class="cov-panel">
            <div class="cov-panel-head"><b>Shared code across ${num(n)} files</b>${legend()}</div>
            <div class="dim" style="font-size:0.75rem;">Each column is a lane: functions found in exactly the dotted files. ${m.hidden ? `The ${num(MAX_LANES)} biggest of ${num(m.lanes.length + m.hidden)} lanes shown. ` : ''}Click a column to open the N-way table.</div>
            <div class="cov-scroll"><div class="cov-mx" style="grid-template-columns:minmax(140px, 240px) 80px repeat(${lanes.length}, 26px);">${h}</div></div>
        </div>`;
    }

    function fingerprints(m, medoid) {
        const { cols, lanes, uniq, names } = m;
        const total = lanes.reduce((s, g) => s + g.rows, 0) + Math.max(0, ...uniq);
        const head = `<div class="r hdr"><span>File</span><div class="cov-lanehdr">${lanes.map(g => `<span style="width:${share(g.rows, total)}; border-color:${COLOR[g.tier]}" title="${escapeAttr(laneName(names, g))}"></span>`).join('')}<span style="flex:1; border-color:${COLOR.uniq}">own</span></div><span style="text-align:right">fn</span></div>`;
        const rows = cols.map((col, i) => `<div class="r">
            <span>${col.id === medoid ? '<span style="color:var(--accent)">★</span>' : ''}${EntityRenderer.renderFileName(col.label, col.md5, col.coll)}</span>
            <div class="cov-track">${lanes.map(g => g.mask[i] === '1'
                ? `<span class="go" style="width:${share(g.rows, total)}; background:${COLOR[g.tier]}" title="${escapeAttr(`${laneName(names, g)} · ${g.rows}`)}" onclick="ClusterOverview.openFunctions('all')"></span>`
                : `<span class="miss" style="width:${share(g.rows, total)}" title="${escapeAttr(`Missing: ${laneName(names, g)}`)}"></span>`).join('')}<span style="width:${share(uniq[i], total)}; background:${COLOR.uniq}" title="${escapeAttr(`Only in this file: ${uniq[i]}`)}"></span></div>
            <span class="dim" style="text-align:right">${num(m.totals[i])}</span></div>`).join('');
        return `<div class="cov-panel">
            <div class="cov-panel-head"><b>Members, by the code they carry</b>${legend(true)}</div>
            <div class="dim" style="font-size:0.75rem;">Lanes sit in fixed slots, widest file coverage first, so a hatched gap is a lane this file lacks.</div>
            <div class="cov-scroll"><div class="cov-fp">${head}${rows}</div></div>
        </div>`;
    }

    function legend(withOwn) {
        const items = [['core', 'Core lane'], ['common', 'Common'], ['rare', 'Rare']].concat(withOwn ? [['uniq', 'Only this file']] : []);
        return `<div class="cov-legend">${items.map(([k, l]) => `<span><i style="background:${COLOR[k]}"></i>${l}</span>`).join('')}${withOwn ? '<span><i style="border:1px dashed var(--dim)"></i>Missing</span>' : ''}</div>`;
    }

    // Lanes and per-file sums of one /nway answer.
    function model(d) {
        const cols = (d.columns || []).map(c => {
            const [coll, md5] = [c.id.slice(0, c.id.lastIndexOf(':')), c.id.slice(c.id.lastIndexOf(':') + 1)];
            return { id: `${coll}:file:${md5}`, coll, md5, label: c.file_name || c.label || md5 };
        });
        const n = cols.length;
        const all = Object.values(d.groups || {}).filter(g => g.kind === 'lane' && g.mask);
        all.forEach(g => { g.tier = tierOf(ones(g.mask), n); });
        const shared = all.filter(g => ones(g.mask) > 1).sort((a, b) => b.rows - a.rows);
        const lanes = shared.slice(0, MAX_LANES).sort((a, b) => ones(b.mask) - ones(a.mask) || b.rows - a.rows);
        const uniq = cols.map((_, i) => all.filter(g => ones(g.mask) === 1 && g.mask[i] === '1').reduce((s, g) => s + g.rows, 0));
        const totals = cols.map((_, i) => shared.filter(g => g.mask[i] === '1').reduce((s, g) => s + g.rows, 0) + uniq[i]);
        const k = d.counts || {};
        const code = t => (k[t] || {}).code || 0;
        return {
            n, cols, lanes, uniq, totals, names: {},
            hidden: shared.length - lanes.length,
            // partial counts every row in 2+ files, core included
            counts: { core: code('core'), partial: Math.max(0, code('partial') - code('core')), unique: code('unique') },
            groups: all,
        };
    }

    async function loadLanes(el, v, c) {
        const uuid = c.cluster_uuid;
        // No name/call merge: a merged function spans rows of several file sets, and lane sizes would stop adding up to the tab counts.
        const qs = new URLSearchParams({
            cluster_uuid: uuid, axis: v.axis, columns: 'files', tab: 'all', scope: 'code',
            group: 'lane', group_merge: '', group_lane_depth: '1', group_lane_tier: '0', group_lane_fold: '0', limit: '0',
        });
        if (v.params && v.params.pool) qs.set('pool', v.params.pool);
        else qs.set('collection', v.collection || '');
        const stale = () => v.selectedClusterUuid !== uuid || !el.isConnected;
        let d;
        try {
            const res = await fetch(`/api/bin_sim/nway?${qs.toString()}`);
            d = await res.json().catch(() => ({}));
            if (!res.ok) {
                const msg = d.error === 'too_large'
                    ? `Too many files to align here (${num(d.members)}). The Functions tab groups them by sub-cluster.`
                    : d.message || `N-way diff failed (${res.status})`;
                throw new Error(msg);
            }
        } catch (e) {
            if (stale()) return;
            el.querySelector('#cov-fn-body').innerHTML = `<div class="dim" style="font-size:0.78rem;">${escapeHtml(e.message)}</div>`;
            return;
        }
        if (stale()) return;
        const m = model(d);
        const medoid = c.medoid || '';
        el.querySelector('#cov-fn-total').textContent = `${num(m.counts.core + m.counts.partial + m.counts.unique)} code functions`;
        el.querySelector('#cov-fn-body').innerHTML = fnCardBody(m);
        if (m.counts.unique) el.querySelector('#cov-fn-uniq').innerHTML = button(`Unique code (${num(m.counts.unique)})`, "ClusterOverview.openFunctions('unique')");
        el.querySelector('#cov-lanes').innerHTML = m.lanes.length
            ? matrix(m, medoid) + fingerprints(m, medoid)
            : `<div class="cov-panel dim">These files share no lane of functions.</div>`;
        nameLanes(el, m, stale);
    }

    // LLM lane names, as the Functions tab asks for them; the server caches them, the fallback label stays meanwhile.
    async function nameLanes(el, m, stale) {
        const lanes = m.lanes.filter(g => (g.sample || []).length || g.hint || (g.tags || []).length || (g.fids || []).length)
            .map(g => ({ id: g.id, files: ones(g.mask), total: g.mask.length, rows: g.rows, sample: g.sample || [], hint: g.hint || '', tags: g.tags || [], fids: g.fids || [] }));
        if (!lanes.length) return;
        for (let i = 0; i < 60 && !stale(); i++) {
            let body = null;
            try {
                const res = await fetch('/api/bin_sim/nway/lane_names', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lanes }) });
                body = res.ok ? await res.json() : null;
            } catch (e) { body = null; }
            if (stale() || !body) return;
            Object.assign(m.names, body.names || {});
            const byId = Object.fromEntries(m.lanes.map(g => [g.id, g]));
            el.querySelectorAll('[data-cov-lname]').forEach(x => {
                const g = byId[x.dataset.covLname];
                if (!g) return;
                x.textContent = laneName(m.names, g);
                x.title = x.textContent;
            });
            if (body.status !== 'pending') return;
            await new Promise(r => setTimeout(r, 1000));
        }
    }

    window.ClusterOverview = {
        render(el, v) {
            const c = v.clusterMapByUuid[v.selectedClusterUuid];
            if (!el || !c) return;
            css();
            const fn = v.hasFunctionsTab();
            el.innerHTML = `<div class="cov">
                <div class="cov-hub">${membersCard(v, c)}${metadataCard(c)}${fn ? functionsCard(c) : ''}</div>
                ${fn ? `<div id="cov-lanes" style="display:flex; flex-direction:column; gap:14px;"></div>` : ''}
            </div>`;
            if (fn) loadLanes(el, v, c);
        },

        /** The Functions tab on one of its own tabs (all / core / partial / unique). */
        openFunctions(ntab) {
            const u = new URL(window.location.href);
            u.searchParams.set('ntab', ntab);
            window.history.replaceState({ path: u.pathname + u.search }, '', u.pathname + u.search);
            window.ClusterDetailView.switchTab('functions');
        },
    };
})();
