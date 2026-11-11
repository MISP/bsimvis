/**
 * Overview tab of a file cluster: one card per tab (Members, Metadata,
 * Functions), then the presence matrix (shared lanes x member files) and the
 * code map (each file's lanes in fixed slots, so a gap is a lane the file lacks).
 *
 *   ClusterOverview.render(el, ClusterDetailView);
 *
 * Members and Metadata come from what the view already loaded (the cluster
 * summary, its direct members and its sample of the subtree). The lanes come from one
 * /api/bin_sim/nway call (`columns=files`, lanes closed, no page limit: the
 * groups without their rows), drawn when it answers. A lane or a file opens the
 * Functions tab on the N-way Focus that holds exactly its functions. Colors and
 * icons are the Functions tab's own. Names come off uploaded samples, so every
 * value is escaped.
 */
(function () {
    const MAX_LANES = 15;
    const TOP = 5;
    const MEMBERS = 8;
    const LANE_TIER = 0.8; // the Functions tab's default lane tier
    const RARE = 'color-mix(in srgb, var(--warning, #d97706) 55%, var(--card-bg))';
    // Same icon + color as the Functions tab's All / Core / Partial / Unique.
    const LOOK = {
        all: ['fa-solid fa-layer-group', 'var(--info, #3b82f6)', 'All'],
        core: ['fa-solid fa-bullseye', 'var(--success)', 'Core'],
        partial: ['fa-solid fa-circle-half-stroke', 'var(--warning, #d97706)', 'Partial'],
        unique: ['fa-solid fa-fingerprint', 'var(--token-instruction, #e11d48)', 'Unique'],
    };
    const TIER = { core: LOOK.core[1], common: LOOK.partial[1], rare: RARE };

    const CSS = `
.cov { display:flex; flex-direction:column; gap:14px; padding-bottom:20px; }
.cov-hub { display:grid; grid-template-columns:repeat(auto-fit, minmax(270px, 1fr)); gap:12px; }
.cov-card, .cov-panel { background:var(--card-bg); border:1px solid var(--border); border-radius:10px; padding:14px 16px; display:flex; flex-direction:column; gap:12px; min-width:0; }
.cov-title { display:flex; align-items:center; gap:9px; min-width:0; flex-wrap:wrap; }
.cov-title b { font-size:0.95rem; }
.cov-ico { width:28px; height:28px; border-radius:7px; display:grid; place-items:center; flex-shrink:0; color:var(--c); background:color-mix(in srgb, var(--c) 16%, transparent); font-size:0.85rem; }
.cov-info { color:var(--dim); font-size:0.78rem; cursor:help; }
.cov-info:hover { color:var(--text); }
.cov-big { margin-left:auto; font-size:1.35rem; font-weight:700; font-variant-numeric:tabular-nums; }
.cov-big small { font-size:0.7rem; color:var(--dim); font-weight:400; margin-left:4px; }
.cov-acts { margin-left:auto; display:flex; gap:6px; flex-wrap:wrap; }
.cov-foot { margin-top:auto; display:flex; gap:6px; flex-wrap:wrap; }
.cov-btn { display:inline-flex; align-items:center; gap:6px; }
.cov-btn i { color:var(--c, inherit); }
.cov-list { display:flex; flex-direction:column; gap:2px; font-size:0.8rem; }
.cov-row { display:grid; grid-template-columns:16px minmax(0,1fr) auto 44px; gap:8px; align-items:center; min-width:0; padding:3px 4px; border-radius:5px; }
.cov-row:hover { background:var(--hover); }
.cov-row > * { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cov-arch { font-size:0.66rem; padding:1px 6px; border-radius:4px; border:1px solid var(--border); color:var(--dim); }
.cov-more { font-size:0.72rem; color:var(--dim); padding-left:28px; }
.cov-sbar { display:flex; gap:2px; height:10px; border-radius:4px; overflow:hidden; background:var(--hover); }
.cov-sbar > span { display:block; min-width:3px; }
.cov-sbar.big { height:12px; border-radius:6px; background:none; }
.cov-sbar.big > span { border-radius:3px; min-width:4px; }
.cov-sbar.big > span:hover { filter:brightness(1.2); }
.cov-mleg { display:flex; flex-wrap:wrap; gap:3px 12px; font-size:0.74rem; color:var(--dim); margin-top:6px; }
.cov-mleg span { display:inline-flex; align-items:center; gap:5px; min-width:0; max-width:100%; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.cov-mleg i { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
.cov-mleg b { color:var(--text); font-weight:600; }
.cov-h3 { font-size:0.62rem; text-transform:uppercase; letter-spacing:1px; color:var(--dim); margin-bottom:5px; }
.cov-fpm { display:flex; gap:8px 16px; flex-wrap:wrap; font-size:0.78rem; color:var(--dim); }
.cov-fpm i, .cov-h3 i { margin-right:3px; }
.cov-fpm b { color:var(--text); }
.cov-chips { display:flex; gap:5px; flex-wrap:wrap; }
.cov-chip { font-size:0.68rem; padding:1px 7px 1px 5px; border-radius:10px; border:1px solid var(--border); white-space:nowrap; max-width:100%; overflow:hidden; text-overflow:ellipsis; display:inline-flex; align-items:center; gap:5px; }
.cov-chip::before { content:''; width:7px; height:7px; border-radius:50%; background:var(--c); flex-shrink:0; }
.cov-tiles { display:grid; grid-template-columns:repeat(3, minmax(0,1fr)); gap:6px; }
.cov-tile { text-align:left; font:inherit; color:var(--text); cursor:pointer; border:1px solid color-mix(in srgb, var(--c) 40%, var(--border)); background:color-mix(in srgb, var(--c) 9%, transparent); border-radius:7px; padding:7px 9px; display:flex; flex-direction:column; gap:1px; min-width:0; }
.cov-tile:hover { background:color-mix(in srgb, var(--c) 18%, transparent); }
.cov-tile span { font-size:0.7rem; color:var(--c); white-space:nowrap; }
.cov-tile b { font-size:1.15rem; font-variant-numeric:tabular-nums; }
.cov-lane { display:grid; grid-template-columns:minmax(50%,1fr) minmax(0,max-content) 36px; gap:8px; align-items:center; font-size:0.78rem; padding:3px 4px; border-radius:5px; cursor:pointer; }
.cov-lane:hover { background:var(--hover); }
.cov-lane > span:first-child { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cov-dots { display:flex; gap:1px; min-width:0; }
.cov-dots i { flex:0 1 5px; min-width:1px; height:11px; border-radius:1px; background:var(--hover); display:block; }
.cov-legend { display:flex; gap:12px; flex-wrap:wrap; font-size:0.72rem; color:var(--dim); }
.cov-legend span { display:inline-flex; align-items:center; gap:5px; }
.cov-legend .sw { width:10px; height:10px; border-radius:2px; display:inline-block; }
.cov-scroll { overflow-x:auto; }
.cov-mx { display:grid; font-size:0.76rem; width:max-content; }
.cov-mx > div { min-width:0; }
.cov-mx .bar { display:flex; flex-direction:column; justify-content:flex-end; align-items:center; gap:2px; height:92px; cursor:pointer; font-size:0.66rem; color:var(--dim); border-radius:4px 4px 0 0; }
.cov-mx .bar:hover { background:var(--hover); color:var(--text); }
.cov-mx .bar i { display:block; width:16px; border-radius:3px 3px 0 0; }
.cov-mx .name { display:flex; align-items:center; gap:5px; padding:0 10px 0 4px; height:24px; overflow:hidden; white-space:nowrap; }
.cov-mx .tot { display:flex; align-items:center; gap:5px; justify-content:flex-end; padding-right:10px; height:24px; color:var(--dim); font-size:0.68rem; font-variant-numeric:tabular-nums; }
.cov-mx .tot i { display:block; height:8px; border-radius:2px; background:var(--dim); opacity:.55; }
.cov-mx .cell { position:relative; height:24px; display:flex; align-items:center; justify-content:center; cursor:pointer; }
.cov-mx .cell:hover { background:var(--hover); }
.cov-mx .cell::before { content:''; position:absolute; left:50%; width:2px; margin-left:-1px; background:var(--ln); }
.cov-mx .cell.full::before { top:0; bottom:0; }
.cov-mx .cell.lo::before { top:50%; bottom:0; }
.cov-mx .cell.hi::before { top:0; bottom:50%; }
.cov-mx .cell b { position:relative; width:11px; height:11px; border-radius:50%; background:var(--ln); box-shadow:0 0 0 2px var(--card-bg); }
.cov-mx .cell s { position:relative; width:5px; height:5px; border-radius:50%; background:var(--border); }
.cov-mx .uq { height:24px; display:flex; align-items:center; justify-content:center; gap:4px; color:${LOOK.unique[1]}; font-size:0.7rem; cursor:pointer; font-variant-numeric:tabular-nums; border-left:1px solid var(--border); }
.cov-mx .uq:hover { background:var(--hover); }
.cov-mx .uqh { height:92px; display:flex; align-items:flex-end; justify-content:center; padding-bottom:4px; color:${LOOK.unique[1]}; border-left:1px solid var(--border); }
.cov-mx .lbl { writing-mode:vertical-rl; transform:rotate(180deg); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-height:160px; padding:8px 0 0; color:var(--dim); font-size:0.7rem; justify-self:center; cursor:pointer; }
.cov-mx .lbl:hover { color:var(--text); }
.cov-mx .stripe { background:color-mix(in srgb, var(--hover) 55%, transparent); }
.cov-fp { display:flex; flex-direction:column; gap:4px; min-width:560px; }
.cov-fp .r { display:grid; grid-template-columns:210px minmax(0,1fr) 48px; gap:10px; align-items:center; font-size:0.78rem; }
.cov-fp .r > span:first-child { overflow:hidden; white-space:nowrap; display:flex; gap:5px; align-items:center; }
.cov-fp .hdr { color:var(--dim); font-size:0.62rem; text-transform:uppercase; letter-spacing:1px; }
.cov-track { display:flex; gap:2px; height:18px; }
.cov-track > span { height:100%; border-radius:3px; min-width:4px; cursor:pointer; }
.cov-track > span:hover { filter:brightness(1.2); outline:1px solid var(--text); }
.cov-track > span.miss { background:repeating-linear-gradient(135deg, transparent 0 3px, color-mix(in srgb, var(--dim) 35%, transparent) 3px 4px); border:1px dashed color-mix(in srgb, var(--dim) 60%, transparent); }
.cov-lanehdr { display:flex; gap:2px; }
.cov-lanehdr > span { min-width:4px; border-top:3px solid; border-radius:2px; }
.cov-wait { color:var(--dim); font-size:0.78rem; padding:6px 0; }
.cov .nway-busy.inline { width:auto; }
`;

    function css() {
        if (document.getElementById('cov-css')) return;
        const s = document.createElement('style');
        s.id = 'cov-css';
        s.textContent = CSS;
        document.head.appendChild(s);
    }

    let cur = null; // the drawn lanes model, for the click handlers

    const num = v => Number(v || 0).toLocaleString();
    const ones = mask => mask.split('1').length - 1;
    const share = (v, total) => `${total ? (v / total * 100).toFixed(2) : 0}%`;
    const spin = text => `<div class="cov-wait"><i class="fa-solid fa-spinner fa-spin"></i> ${escapeHtml(text)}</div>`;
    const go = js => `onclick="${escapeAttr(js)}"`;

    // Same bands as the server's lane tiers (nway_group_service._lane_tiers).
    function tierOf(files, n) {
        const core = Math.max(2, Math.ceil(LANE_TIER * n));
        const common = Math.max(2, Math.ceil(core / 2));
        return files >= core ? 'core' : files >= common ? 'common' : 'rare';
    }

    const title = (icon, color, name, info, right) => `<div class="cov-title">
        <span class="cov-ico" style="--c:${color}"><i class="${icon}"></i></span><b>${name}</b>
        <i class="fa-solid fa-circle-info cov-info" title="${escapeAttr(info)}"></i>${right || ''}</div>`;
    const btn = (icon, color, label, js, tip) => `<button class="btn-code-action cov-btn" style="--c:${color}" ${tip ? `title="${escapeAttr(tip)}"` : ''} ${go(js)}><i class="${icon}"></i> ${escapeHtml(label)}</button>`;
    const sbar = parts => `<div class="cov-sbar">${parts.filter(p => p[1] > 0)
        .map(([color, v, label]) => `<span style="flex:${Number(v)}; background:${color}" title="${escapeAttr(`${label}: ${num(v)}`)}"></span>`).join('')}</div>`;
    const fullBtn = () => btn('fa-solid fa-expand', 'var(--accent)', 'Full screen', 'ClusterDetailView.openFunctionsFull(event)', 'Open the N-way table on its own page');

    // ---- Members: the main file, the most central direct members, then the summary's sample of the subtree ----
    // ponytail: no file search here -- its bin_cluster_uuid index is empty for any node with children.
    function membersCard(v, c) {
        const direct = v.memberCache[c.cluster_uuid] || [];
        const kids = (v.childrenMap[c.cluster_id] || []).length;
        const med = c.medoid || '';
        const mf = (v._medoidFiles || {})[med] || {};
        const named = Object.fromEntries([...(c.sample_members || []), ...direct].map(m => [m.id, m.file_name || m.name]));
        const known = Object.fromEntries(direct.map(m => [m.id, m]));
        const seen = new Set(), files = [];
        const add = (id, name, arch, cen) => {
            if (!id || seen.has(id)) return;
            seen.add(id);
            const k = known[id] || {};
            files.push({ id, name: name || named[id], arch: arch || k.language_id, cen: cen ?? k.centrality });
        };
        if (med) add(med, mf.file_name, mf.language_id);
        [...direct].sort((a, b) => (b.centrality ?? -1) - (a.centrality ?? -1)).forEach(m => add(m.id, m.name, m.language_id, m.centrality));
        (c.sample_members || []).forEach(m => add(m.id, m.file_name || m.name));
        const total = Number(c.count || 0);
        const rows = files.slice(0, MEMBERS).map(f => {
            const at = String(f.id).lastIndexOf(':file:');
            const coll = at > 0 ? f.id.slice(0, at) : v.collection || '';
            const md5 = String(f.id).split(':').pop();
            const arch = String(f.arch || '').split(':')[0];
            return `<div class="cov-row">
                <span style="color:var(--accent)" ${f.id === med ? 'title="Main file (medoid)"' : ''}>${f.id === med ? '★' : ''}</span>
                ${EntityRenderer.renderFileName(f.name || md5, md5, coll)}
                <span>${arch ? `<span class="cov-arch">${escapeHtml(arch)}</span>` : ''}</span>
                <span class="dim" style="text-align:right" ${f.cen == null ? '' : 'title="Centrality"'}>${f.cen == null ? '' : Number(f.cen).toFixed(2)}</span></div>`;
        }).join('');
        const more = [total > Math.min(MEMBERS, files.length) ? `+${num(total - Math.min(MEMBERS, files.length))} more` : '', kids ? `${num(kids)} sub-cluster${kids > 1 ? 's' : ''}` : ''].filter(Boolean).join(' · ');
        return `<div class="cov-card">
            ${title('fa-solid fa-users', 'var(--accent)', 'Members', 'Every file of this cluster and its sub-clusters. ★ is the main file, the most central one.', `<span class="cov-big">${num(total)}<small>files</small></span>`)}
            <div class="cov-list">${rows || '<div class="dim">No members listed.</div>'}${more ? `<div class="cov-more">${more}</div>` : ''}</div>
            <div class="cov-foot">${btn('fa-solid fa-list', 'var(--accent)', 'All members', "ClusterDetailView.switchTab('members')")}</div>
        </div>`;
    }

    // `AARCH64:LE:64:v8A` -> `AARCH64 64-bit`; `Executable and Linking Format (ELF)` -> `ELF`.
    const shortValue = (key, value) => {
        const v = String(value ?? '');
        if (key === 'arch') {
            const p = v.split(':');
            return p.length > 2 ? `${p[0]} ${p[2]}-bit` : v;
        }
        return (v.match(/\(([^)]+)\)\s*$/) || [null, v])[1];
    };

    // One distribution as a stacked coverage bar, with a legend of its top values (none for uuid-valued ones).
    function distBlock(label, icon, key, d) {
        const sorted = [...d].sort((a, b) => b.count - a.count);
        const color = (it, i) => escapeAttr(metadataColor(it, i));
        const bar = `<div class="cov-sbar big">${sorted.map((it, i) => `<span style="flex:${Number(it.count)}; background:${color(it, i)}" title="${escapeAttr(`${it.value}: ${it.count} (${metadataPercent(it).toFixed(0)}%)`)}"></span>`).join('')}</div>`;
        const leg = key === 'batch' ? '' : `<div class="cov-mleg">${sorted.slice(0, 3).map((it, i) => `<span title="${escapeAttr(String(it.value))}"><i style="background:${color(it, i)}"></i>${escapeHtml(shortValue(key, it.value))} <b>${metadataPercent(it).toFixed(0)}%</b></span>`).join('')}${sorted.length > 3 ? `<span class="dim">+${num(sorted.length - 3)}</span>` : ''}</div>`;
        return `<div><div class="cov-h3"><i class="${icon}"></i> ${label} <span style="float:right">${num(d.length)}</span></div>${bar}${leg}</div>`;
    }

    // ---- Metadata: the cluster summary ----
    function metadataCard(c) {
        const rows = [['Architecture', 'fa-solid fa-microchip', 'arch', c.architecture_distribution], ['Format', 'fa-solid fa-file-code', 'format', c.executable_format_distribution], ['Batch', 'fa-solid fa-box', 'batch', c.batch_uuid_distribution]]
            .filter(([, , , d]) => d && d.length).map(a => distBlock(...a)).join('');
        const tags = [c.tag_distribution, c.file_tag_distribution].flatMap(by => Object.values(by || {}).flat())
            .sort((a, b) => metadataPercent(b) - metadataPercent(a)).slice(0, 6);
        const s = c.function_count_stats || {};
        const fpm = s.min === undefined ? '' : `<div class="cov-fpm"><span title="Functions per member"><i class="fa-solid fa-list-ol"></i> ${[['min', s.min], ['avg', s.avg], ['max', s.max]].map(([l, x]) => `${l} <b>${num(Math.round(x))}</b>`).join(' · ')}</span></div>`;
        const archs = (c.architecture_distribution || []).length;
        return `<div class="cov-card">
            ${title('fa-solid fa-chart-pie', 'var(--info, #3b82f6)', 'Metadata', 'How the member files split by architecture, format and upload batch, and the tags most of them carry.', archs ? `<span class="cov-big">${num(archs)}<small>arch${archs > 1 ? 's' : ''}</small></span>` : '')}
            ${rows ? `<div style="display:flex; flex-direction:column; gap:10px;">${rows}</div>` : `<div class="dim" style="font-size:0.78rem;">No distributions stored. A cluster below the configured min_cohesion reports none.</div>`}
            ${fpm}
            ${tags.length ? `<div class="cov-chips">${tags.map((t, i) => `<span class="cov-chip" style="--c:${escapeAttr(metadataColor(t, i, 'tag_id'))}" title="${escapeAttr(t.tag_id)}">${escapeHtml(t.tag_id)} <span class="dim">${metadataPercent(t).toFixed(0)}%</span></span>`).join('')}</div>` : ''}
            <div class="cov-foot">${btn('fa-solid fa-chart-pie', 'var(--info, #3b82f6)', 'All distributions', "ClusterDetailView.switchTab('metadata')")}</div>
        </div>`;
    }

    // ---- Functions: N-way counts and the biggest lanes ----
    function functionsCard(c) {
        return `<div class="cov-card">
            ${title(LOOK.all[0], LOOK.all[1], 'Functions', 'Code functions aligned across the member files. Core: in every file. Partial: in some. Unique: in one file only.', `<span class="cov-big" id="cov-fn-total"></span>`)}
            <div id="cov-fn-body" style="display:flex; flex-direction:column; gap:10px;">${spin(`Aligning functions across ${num(c.count)} files…`)}</div>
            <div class="cov-foot">${btn(LOOK.all[0], LOOK.all[1], 'Functions', "ClusterOverview.openFunctions({ tab: 'all' })")}${fullBtn()}</div>
        </div>`;
    }

    // ---- Lineage: a small tree of how the member files derive from each other ----
    function lineageCard() {
        return `<div class="cov-card">
            ${title('fa-solid fa-code-branch', 'var(--accent)', 'Lineage', 'Which member file derives from which, by shared functions. Opens the full lineage page.', '')}
            <div id="cov-lin-body" style="min-height:110px;">${spin('Tracing lineage…')}</div>
            <div class="cov-foot">${btn('fa-solid fa-code-branch', 'var(--accent)', 'Lineage', 'ClusterOverview.openLineage(event)', 'Open the lineage page')}</div>
        </div>`;
    }

    function lineageSvg(doc) {
        const nodes = doc.nodes || [], kids = {};
        (doc.edges || []).forEach(e => (kids[e.parent] = kids[e.parent] || []).push(e.child));
        const byId = Object.fromEntries(nodes.map(n => [n.id, n]));
        const MAX = 40, pos = {};
        let row = 0, maxD = 0;
        const walk = (id, d) => {
            if (row >= MAX || pos[id]) return;
            pos[id] = { d, r: row++ };
            maxD = Math.max(maxD, d);
            (kids[id] || []).forEach(k => walk(k, d + 1));
        };
        walk(doc.root, 0);
        const W = 260, H = Math.max(60, row * 12 + 16), sx = maxD ? (W - 40) / maxD : 0;
        const xy = id => [20 + pos[id].d * sx, 10 + pos[id].r * 12];
        const lines = (doc.edges || []).filter(e => pos[e.parent] && pos[e.child]).map(e => {
            const [x1, y1] = xy(e.parent), [x2, y2] = xy(e.child);
            return `<path d="M${x1} ${y1}H${(x1 + x2) / 2}V${y2}H${x2}" fill="none" stroke="var(--border)" stroke-width="1.5"/>`;
        }).join('');
        const dots = Object.keys(pos).map(id => {
            const [x, y] = xy(id);
            const root = id === doc.root;
            return `<circle cx="${x}" cy="${y}" r="${root ? 5 : 3.5}" fill="${root ? 'var(--accent)' : 'var(--info, #3b82f6)'}"><title>${escapeHtml((byId[id] || {}).file_name || id)}</title></circle>`;
        }).join('');
        const more = nodes.length > row ? `<div class="dim" style="font-size:0.72rem;">Showing ${row} of ${num(nodes.length)} files</div>` : '';
        return `<div style="overflow:auto; max-height:200px;"><svg viewBox="0 0 ${W} ${H}" width="100%" style="max-width:${W}px" role="img" aria-label="Lineage tree">${lines}${dots}</svg></div>${more}`;
    }

    async function loadLineage(el, v, c) {
        const body = el.querySelector('#cov-lin-body');
        if (!body) return;
        const stale = () => !el.isConnected || v.selectedClusterUuid !== c.cluster_uuid;
        try {
            const qs = new URLSearchParams({ cluster_uuid: c.cluster_uuid, axis: v.axis, columns: 'files', order: 'tree' });
            if (v.params && v.params.pool) qs.set('pool', v.params.pool); else qs.set('collection', v.collection || '');
            const res = await fetch(`/api/bin_sim/nway/lineage?${qs}`);
            const doc = await res.json();
            if (stale()) return;
            if (!res.ok || !(doc.nodes || []).length) throw new Error(doc.message || doc.error || `HTTP ${res.status}`);
            body.innerHTML = lineageSvg(doc);
        } catch (e) {
            if (!stale()) body.innerHTML = `<div class="dim" style="font-size:0.78rem;">No preview for this cluster. Open the lineage page to build it.</div>`;
        }
    }

    const laneName = (names, g) => (names[g.id] && !/^unnamed/i.test(names[g.id])) ? names[g.id] : g.hint ? `${g.hint}…` : `${ones(g.mask)}-file lane`;
    const laneTip = (g, n) => `${g.rows} functions in ${ones(g.mask)} of ${n} files. Click to open them.`;
    const laneGo = g => go(`ClusterOverview.openLane(${jsString(g.id)})`);

    function fnCardBody(m) {
        const tile = (key, v) => `<button class="cov-tile" style="--c:${LOOK[key][1]}" ${go(`ClusterOverview.openFunctions({ tab: '${key}' })`)}><span><i class="${LOOK[key][0]}"></i> ${LOOK[key][2]}</span><b>${num(v)}</b></button>`;
        const k = m.counts;
        const dots = g => `<span class="cov-dots">${[...g.mask].map(b => `<i style="${b === '1' ? `background:${TIER[g.tier]}` : ''}"></i>`).join('')}</span>`;
        const top = [...m.lanes].sort((a, b) => b.rows - a.rows).slice(0, TOP);
        return `<div class="cov-tiles">${tile('core', k.core)}${tile('partial', k.partial)}${tile('unique', k.unique)}</div>
            ${sbar([[LOOK.core[1], k.core, 'Core'], [LOOK.partial[1], k.partial, 'Partial'], [LOOK.unique[1], k.unique, 'Unique']])}
            ${top.length ? `<div><div class="cov-h3">Biggest shared lanes</div>${top.map(g => `<div class="cov-lane" title="${escapeAttr(laneTip(g, m.n))}" ${laneGo(g)}>
                <span data-cov-lname="${escapeAttr(g.id)}">${escapeHtml(laneName(m.names, g))}</span>${m.n <= 40 ? dots(g) : `<span class="dim">${ones(g.mask)}/${m.n}</span>`}<span class="dim" style="text-align:right">${num(g.rows)}</span></div>`).join('')}</div>` : ''}`;
    }

    function legend(withMiss) {
        return `<div class="cov-legend">${[['Core lane', TIER.core], ['Common', TIER.common], ['Rare', TIER.rare]].map(([l, c]) => `<span><i class="sw" style="background:${c}"></i>${l}</span>`).join('')}
            <span style="color:${LOOK.unique[1]}"><i class="${LOOK.unique[0]}"></i>Only this file</span>${withMiss ? '<span><i class="sw" style="border:1px dashed var(--dim)"></i>Missing</span>' : ''}</div>`;
    }

    const panelActs = m => `<div class="cov-acts">${btn(LOOK.all[0], LOOK.all[1], 'Functions', "ClusterOverview.openFunctions({ tab: 'all' })")}
        ${btn(LOOK.core[0], LOOK.core[1], `Core ${num(m.counts.core)}`, "ClusterOverview.openFunctions({ tab: 'core' })")}
        ${btn(LOOK.unique[0], LOOK.unique[1], `Unique ${num(m.counts.unique)}`, "ClusterOverview.openFunctions({ tab: 'unique' })")}${fullBtn()}</div>`;

    function matrix(m, medoid) {
        const { n, cols, lanes, names } = m;
        const maxRows = Math.max(1, ...lanes.map(g => g.rows));
        const maxTot = Math.max(1, ...m.totals);
        const span = lanes.map(g => [g.mask.indexOf('1'), g.mask.lastIndexOf('1')]);
        let h = `<div></div><div class="tot" style="align-items:flex-end; height:92px; padding-bottom:4px;">functions</div>`;
        h += lanes.map(g => `<div class="bar" title="${escapeAttr(laneTip(g, n))}" ${laneGo(g)}><span>${num(g.rows)}</span><i style="height:${Math.max(3, g.rows / maxRows * 66)}px; background:${TIER[g.tier]}"></i></div>`).join('');
        h += `<div class="uqh" title="Functions only this file has"><i class="${LOOK.unique[0]}"></i></div>`;
        cols.forEach((col, i) => {
            const st = i % 2 ? '' : ' stripe';
            h += `<div class="name${st}">${col.id === medoid ? '<span style="color:var(--accent)" title="Main file (medoid)">★</span>' : ''}${EntityRenderer.renderFileName(col.label, col.md5, col.coll)}</div>`;
            h += `<div class="tot${st}"><i style="width:${m.totals[i] / maxTot * 38}px"></i>${num(m.totals[i])}</div>`;
            h += lanes.map((g, j) => {
                const [a, b] = span[j];
                const line = a === b ? '' : i === a ? ' lo' : i === b ? ' hi' : i > a && i < b ? ' full' : '';
                return `<div class="cell${st}${line}" style="--ln:${TIER[g.tier]}" title="${escapeAttr(`${laneName(names, g)}: ${g.mask[i] === '1' ? 'present' : 'absent'} in ${col.label}`)}" ${laneGo(g)}>${g.mask[i] === '1' ? '<b></b>' : '<s></s>'}</div>`;
            }).join('');
            h += `<div class="uq${st}" title="${escapeAttr(`${m.uniq[i]} functions only in ${col.label}. Click to open them.`)}" ${go(`ClusterOverview.openFile(${i})`)}>${m.uniq[i] ? num(m.uniq[i]) : ''}</div>`;
        });
        h += `<div></div><div></div>` + lanes.map(g => `<div class="lbl" title="${escapeAttr(laneName(names, g))}" data-cov-lname="${escapeAttr(g.id)}" ${laneGo(g)}>${escapeHtml(laneName(names, g))}</div>`).join('') + `<div class="lbl" style="color:${LOOK.unique[1]}">Unique</div>`;
        const info = `Each column is a lane: functions found in exactly the dotted files. Click a column, a dot or a label to open that lane's functions in the table; click a red count for the functions only that file has.${m.hidden ? ` The ${MAX_LANES} biggest of ${m.lanes.length + m.hidden} shared lanes are shown.` : ''}`;
        return `<div class="cov-panel">
            ${title('fa-solid fa-diagram-project', 'var(--success)', 'Shared code', info, panelActs(m))}
            ${legend()}
            <div class="cov-scroll"><div class="cov-mx" style="grid-template-columns:minmax(150px, 250px) 84px repeat(${lanes.length}, 28px) 44px;">${h}</div></div>
        </div>`;
    }

    function codeMap(m, medoid) {
        const { cols, lanes, uniq, names } = m;
        const total = lanes.reduce((s, g) => s + g.rows, 0) + Math.max(0, ...uniq);
        const head = `<div class="r hdr"><span>File</span><div class="cov-lanehdr">${lanes.map(g => `<span style="width:${share(g.rows, total)}; border-color:${TIER[g.tier]}" title="${escapeAttr(laneName(names, g))}"></span>`).join('')}<span style="flex:1; border-color:${LOOK.unique[1]}"></span></div><span style="text-align:right">fn</span></div>`;
        const rows = cols.map((col, i) => `<div class="r">
            <span>${col.id === medoid ? '<span style="color:var(--accent)">★</span>' : ''}${EntityRenderer.renderFileName(col.label, col.md5, col.coll)}</span>
            <div class="cov-track">${lanes.map(g => g.mask[i] === '1'
                ? `<span style="width:${share(g.rows, total)}; background:${TIER[g.tier]}" title="${escapeAttr(`${laneName(names, g)} · ${g.rows}`)}" ${laneGo(g)}></span>`
                : `<span class="miss" style="width:${share(g.rows, total)}" title="${escapeAttr(`Missing: ${laneName(names, g)}`)}" ${laneGo(g)}></span>`).join('')}<span style="width:${share(uniq[i], total)}; background:${LOOK.unique[1]}" title="${escapeAttr(`Only in this file: ${uniq[i]}`)}" ${go(`ClusterOverview.openFile(${i})`)}></span></div>
            <span class="dim" style="text-align:right">${num(m.totals[i])}</span></div>`).join('');
        return `<div class="cov-panel">
            ${title('fa-solid fa-barcode', LOOK.partial[1], 'Code map', "Each file's lanes in fixed slots, widest file coverage first. A hatched slot is a lane the file lacks; red is code only that file has. Click a slot to open its functions.", panelActs(m))}
            ${legend(true)}
            <div class="cov-scroll"><div class="cov-fp">${head}${rows}</div></div>
        </div>`;
    }

    // Lanes and per-file sums of one /nway answer.
    function model(d) {
        const cols = (d.columns || []).map(c => {
            const at = c.id.lastIndexOf(':');
            const coll = c.id.slice(0, at), md5 = c.id.slice(at + 1);
            return { key: c.id, id: `${coll}:file:${md5}`, coll, md5, label: c.file_name || c.label || md5 };
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
            byId: Object.fromEntries(lanes.map(g => [g.id, g])),
            hidden: shared.length - lanes.length,
            // partial counts every row in 2+ files, core included
            counts: { core: code('core'), partial: Math.max(0, code('partial') - code('core')), unique: code('unique') },
        };
    }

    // The last lanes call, shared while it runs and kept once it answered, so leaving and
    // re-entering the tab never sends it twice. A failed call is dropped, the next render retries.
    let flight = null;
    function lanesCall(qs) {
        const key = qs.toString();
        if (flight && flight.key === key) return flight;
        const pid = NwayPanel.progressId(), stop = new AbortController(), t0 = Date.now();
        const f = flight = { key, pid, stop, answer: null };
        f.promise = fetch(`/api/bin_sim/nway?${key}&progress_id=${pid}`, { signal: AbortSignal.any([stop.signal, AbortSignal.timeout(NwayPanel.TIMEOUT_MS)]) })
            .then(async res => ({ ok: res.ok, d: await res.json().catch(() => ({})) }))
            .catch(e => ({ ok: false, d: { message: stop.signal.aborted ? 'Cancelled.' : e.name === 'TimeoutError' ? `Timed out after ${NwayPanel.TIMEOUT_MS / 1000}s matching functions.` : String(e) } }))
            .then(r => {
                if (r.ok) { f.answer = r; NwayPanel._lastMs = Date.now() - t0; }
                else if (flight === f) flight = null;
                return r;
            });
        return f;
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
        const f = lanesCall(qs);
        let done = null;
        if (!f.answer) {
            const host = el.querySelector('#cov-fn-body');
            host.innerHTML = '';
            done = NwayPanel.busy(host, f.pid, { inline: true, onCancel: () => f.stop.abort() });
        }
        const { ok, d } = await f.promise;
        if (done) done();
        if (stale()) return;
        if (!ok) {
            const msg = d.error === 'too_large'
                ? `Too many files to align here (${num(d.members)}). The Functions tab groups them by sub-cluster.`
                : d.message || 'N-way diff failed.';
            el.querySelector('#cov-fn-body').innerHTML = `<div class="dim" style="font-size:0.78rem;">${escapeHtml(msg)} ${btn('fa-solid fa-rotate-right', 'var(--accent)', 'Retry', 'ClusterDetailView.renderOverviewTab()')}</div>`;
            return;
        }
        const m = cur = model(d);
        const medoid = c.medoid || '';
        el.querySelector('#cov-fn-total').innerHTML = `${num(m.counts.core + m.counts.partial + m.counts.unique)}<small>code fn</small>`;
        el.querySelector('#cov-fn-body').innerHTML = fnCardBody(m);
        el.querySelector('#cov-lanes').innerHTML = m.lanes.length
            ? matrix(m, medoid) + codeMap(m, medoid)
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
            el.querySelectorAll('[data-cov-lname]').forEach(x => {
                const g = m.byId[x.dataset.covLname];
                if (!g) return;
                x.textContent = laneName(m.names, g);
                x.title = x.textContent;
            });
            if (body.status !== 'pending') return;
            await new Promise(r => setTimeout(r, 1000));
        }
    }

    // URL keys of the Functions tab's Focus; cleared unless the link sets them.
    const FOCUS_KEYS = ['focus', 'focus_rule', 'side', 'child_presence'];

    window.ClusterOverview = {
        render(el, v) {
            const c = v.clusterMapByUuid[v.selectedClusterUuid];
            if (!el || !c) return;
            css();
            cur = null;
            const fn = v.hasFunctionsTab();
            el.innerHTML = `<div class="cov">
                <div class="cov-hub">${membersCard(v, c)}${metadataCard(c)}${fn ? functionsCard(c) + lineageCard() : ''}</div>
                ${fn ? `<div id="cov-lanes" style="display:flex; flex-direction:column; gap:14px;"></div>` : ''}
            </div>`;
            if (fn) { loadLanes(el, v, c); loadLineage(el, v, c); }
        },

        /** The Functions tab with these N-way controls (`tab` is its All / Core / Partial / Unique). */
        openFunctions(state) {
            const u = new URL(window.location.href);
            u.searchParams.set('ntab', state.tab || 'all');
            FOCUS_KEYS.forEach(k => (state[k] == null ? u.searchParams.delete(k) : u.searchParams.set(k, state[k])));
            window.history.replaceState({ path: u.pathname + u.search }, '', u.pathname + u.search);
            window.ClusterDetailView.switchTab('functions');
        },

        openLineage(event) {
            const v = window.ClusterDetailView;
            const qs = new URLSearchParams({ cluster_uuid: v.selectedClusterUuid, axis: v.axis });
            if (v.params && v.params.pool) qs.set('pool', v.params.pool); else qs.set('collection', v.collection || '');
            window.Nav.openPath(`/diff/nway/lineage?${qs}`, event, { title: 'Lineage', type: 'bin_sim' });
        },

        /** A lane's functions: Focus on its files, all of them required, none of the rest. */
        openLane(id) {
            const g = cur && cur.byId[id];
            if (!g) return this.openFunctions({ tab: 'all' });
            const ids = cur.cols.filter((_, i) => g.mask[i] === '1').map(c => c.key);
            // Focus needs a file on each side; a lane in every file is the Core tab.
            if (ids.length >= cur.n) return this.openFunctions({ tab: 'core' });
            this.openFunctions({ tab: 'unique', side: 'focus', focus_rule: 'all', focus: ids.join(','), child_presence: '0.05' });
        },

        /** The functions only one file has. */
        openFile(i) {
            const col = cur && cur.cols[i];
            if (!col || cur.n < 2) return this.openFunctions({ tab: 'unique' });
            this.openFunctions({ tab: 'unique', side: 'focus', focus_rule: 'any', focus: col.key, child_presence: '0.05' });
        },
    };
})();
