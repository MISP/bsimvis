/**
 * Collection Detail View
 * Loaded when navigating to /collections/{id}
 */

window.CollectionDetailView = {
    refreshInterval: null,

    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
    },

    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const collName = params.collection;
        this._ovForce = !!params.force;
        if (!collName) {
            container.innerHTML = '<div style="padding:30px; color:#f87171;">Error: No collection name provided.</div>';
            return;
        }

        container.innerHTML = `
            <div style="display:flex; justify-content:center; align-items:center; height:200px; color:var(--dim); font-size:1rem;">
                <i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading Collection Details...
            </div>`;

        try {
            const [collRes, poolsRes, jobsStatsRes, paramsRes, countsRes] = await Promise.all([
                fetch(`/api/collection/search?q=${encodeURIComponent(collName)}`),
                fetch(`/api/pool?collection=${encodeURIComponent(collName)}`),
                fetch('/api/jobs/stats').catch(() => null),
                fetch(`/api/collection/params?collection=${encodeURIComponent(collName)}`).catch(() => null),
                fetch(`/api/collection/counts?collection=${encodeURIComponent(collName)}`).catch(() => null)
            ]);

            if (!collRes.ok) throw new Error('Failed to load collections list');
            const data = await collRes.json();
            const collection = (data.collections || []).find(c => c.name === collName);

            let associatedPools = [];
            if (poolsRes.ok) {
                const pd = await poolsRes.json();
                associatedPools = pd.pools || [];
            }

            let params = {};
            if (paramsRes && paramsRes.ok) {
                const pj = await paramsRes.json();
                params = pj.params || {};
            }

            const counts = countsRes && countsRes.ok ? await countsRes.json() : {};

            let collectionActiveJobs = [];
            if (jobsStatsRes && jobsStatsRes.ok) {
                const stats = await jobsStatsRes.json();
                const activeJobs = stats.active_jobs || [];
                collectionActiveJobs = activeJobs.filter(job => job.collection === collName);
            }

            if (!collection) {
                container.innerHTML = this._renderPage({
                    name: collName,
                    total_files: 0,
                    total_functions: 0,
                    total_batches: 0,
                    last_updated: 0,
                    counts
                }, associatedPools, collectionActiveJobs, params);
            } else {
                container.innerHTML = this._renderPage({ ...collection, counts }, associatedPools, collectionActiveJobs, params);
            }

            this._loadOverview(collName, { ...(collection || {}), counts });

            if (this.refreshInterval) clearInterval(this.refreshInterval);
            this.refreshInterval = setInterval(async () => {
                try {
                    const res = await fetch('/api/jobs/stats');
                    if (res.ok) {
                        const stats = await res.json();
                        const activeJobs = stats.active_jobs || [];
                        const filtered = activeJobs.filter(job => job.collection === collName);
                        const tableContainer = document.getElementById('active-jobs-container');
                        if (tableContainer) {
                            tableContainer.innerHTML = this._renderActiveJobsSection(collName, filtered);
                        }
                    }
                } catch (e) {
                    console.error("Auto refresh jobs error:", e);
                }
            }, 3000);

        } catch (e) {
            container.innerHTML = `<div style="padding:30px; color:#f87171;"><i class="fa-solid fa-triangle-exclamation"></i> ${e.message}</div>`;
        }
    },

    /** Landing cards (cluster Overview look): family map, families, files, uploads, mix. One cached call, nothing computed live. */
    async _loadOverview(col, coll = {}) {
        const host = document.getElementById('coll-overview');
        if (!host) return;
        this._ovCol = col;
        this._ovColl = coll;
        try {
            const qs = new URLSearchParams({ collection: col, axis: this._ovAxis() });
            if (this._ovForce) qs.set('refresh', 'true'); // Refresh button: skip the server's 120 s cache
            this._ovForce = false;
            const res = await fetch(`/api/index/collection/overview?${qs}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            this._ov = await res.json();
        } catch (e) {
            console.error('Overview failed', e);
            this._ov = { error: true };
        }
        if (this._ovCol !== col || !host.isConnected) return;
        host.innerHTML = this._renderOverview();
        this._drawFamilyMap();
    },

    /** Cluster axis the cards read: URL `axis`, else the app default (code). */
    _ovAxis() {
        const a = new URLSearchParams(location.search).get('axis');
        return window.FileTree && FileTree.AXES.includes(a) ? a : (window.BINSIM_DEFAULT_AXIS || 'code');
    },

    /** `?key=axis` for a link, or nothing when the axis is the default its target starts on. */
    _ovAxisQs(key = 'axis') {
        const a = this._ovAxis();
        return a === (window.BINSIM_DEFAULT_AXIS || 'code') ? '' : `?${key}=${encodeURIComponent(a)}`;
    },

    _ovSetAxis(a) {
        const u = new URL(location.href);
        if (a === (window.BINSIM_DEFAULT_AXIS || 'code')) u.searchParams.delete('axis'); else u.searchParams.set('axis', a);
        history.replaceState(history.state, '', u);
        const host = document.getElementById('coll-overview');
        if (host) host.innerHTML = this._ovSkeleton(this._ovCol, this._ovColl);
        this._loadOverview(this._ovCol, this._ovColl);
    },

    /** Same axis buttons as the file tree's cluster grouping (FileTree toolbar). */
    _ovAxisSeg() {
        const types = window.BinSimScoreTypes || {}, cur = this._ovAxis();
        const btns = (window.FileTree ? FileTree.AXES : ['overall', 'code', 'library', 'content']).map(a => {
            const m = types[a === 'overall' ? 'score' : `score_${a}`] || {};
            const label = m.label || a;
            return `<button type="button" class="ft-seg-btn${a === cur ? ' active' : ''}" style="--ft-axis:${m.color || 'var(--accent)'}" aria-pressed="${a === cur}" title="${escapeAttr(`Show ${label} clusters`)}" onclick="CollectionDetailView._ovSetAxis('${a}')"><i class="${m.icon || 'fa-solid fa-layer-group'}"></i> ${escapeHtml(label)}</button>`;
        }).join('');
        return `<div class="cdo-bar"><span class="dim">Clusters by</span><div class="ft-seg" role="group" aria-label="Cluster axis">${btns}</div></div>`;
    },

    /** The overview's cards at their final size, bodies still loading: nothing moves when the data lands. */
    _ovSkeleton(col, coll = {}) {
        this._ov = null;
        this._ovCol = col;
        this._ovColl = coll;
        if (window.ClusterOverview) ClusterOverview.ensureCss();
        if (!document.getElementById('cdo-css')) {
            const st = document.createElement('style');
            st.id = 'cdo-css';
            st.textContent = this._OV_CSS;
            document.head.appendChild(st);
        }
        return this._renderOverview();
    },

    _ovNway(uuid) {
        return `/diff/nway?${new URLSearchParams({ cluster_uuid: uuid, axis: this._ovAxis(), collection: this._ovCol })}`;
    },

    _ovBtn(icon, color, label, url, tip = '') {
        return `<button class="btn-code-action cov-btn" style="--c:${color}" ${tip ? `title="${escapeAttr(tip)}"` : ''} onclick="${escapeAttr(`Nav.openPath(${jsString(url)}, event)`)}"><i class="${icon}"></i> ${escapeHtml(label)}</button>`;
    },

    _ovTitle(icon, color, name, info, big) {
        return `<div class="cov-title"><span class="cov-ico" style="--c:${color}"><i class="${icon}"></i></span><b>${name}</b>
            <i class="fa-solid fa-circle-info cov-info" title="${escapeAttr(info)}"></i>${big === undefined ? '' : `<span class="cov-big">${Number(big || 0).toLocaleString()}</span>`}</div>`;
    },

    // Same cohesion colour as the cluster cards (renderClusterCards).
    _cohColor(c) {
        return `hsl(${Math.max(0, Math.min(120, (c || 0) * 120))}, var(--color-s-high), var(--color-l-high))`;
    },

    _ovFamRow(f, max) {
        const uuid = f.cluster_uuid;
        const name = f.cluster_name || `Cluster ${f.cid}`;
        const card = renderClusterCards([{ cluster_id: f.cid, cluster_uuid: uuid, cluster_name: f.cluster_name, cohesion_score: f.cohesion_score || 0, member_count: f.matching || 0, axis: this._ovAxis() }], true);
        const w = max ? Math.max(3, (f.matching || 0) / max * 100) : 0;
        return `<div class="cdo-fam">
            <span>${card || ''}</span>
            <a class="fb-link" onclick="openCluster(${escapeAttr(jsString(uuid))}, true, event, ${escapeAttr(jsString(this._ovAxis()))})" title="${escapeAttr(name)}">${escapeHtml(name)}</a>
            <span class="cdo-size"><i style="width:${w.toFixed(1)}%"></i></span>
            <a class="cdo-go" onclick="${escapeAttr(`Nav.openPath(${jsString(this._ovNway(uuid))}, event)`)}" title="Functions: N-way diff of this cluster"><i class="fa-solid fa-layer-group"></i></a>
        </div>`;
    },

    /** A family row with an activity figure on the right (Most active / Recently changed). */
    _ovActRow(f, right, tip) {
        const uuid = f.cluster_uuid;
        const name = f.cluster_name || `Cluster ${f.cid}`;
        const card = renderClusterCards([{ cluster_id: f.cid, cluster_uuid: uuid, cluster_name: f.cluster_name, cohesion_score: f.cohesion_score || 0, member_count: f.matching || 0, axis: this._ovAxis() }], true);
        return `<div class="cdo-act">
            <span>${card || ''}</span>
            <a class="fb-link" onclick="openCluster(${escapeAttr(jsString(uuid))}, true, event, ${escapeAttr(jsString(this._ovAxis()))})" title="${escapeAttr(name)}">${escapeHtml(name)}</a>
            <span class="mono" title="${escapeAttr(tip)}">${right}</span>
        </div>`;
    },

    _ovFileRows(files) {
        const col = this._ovCol;
        if (!files.length) return '<div class="dim" style="font-size:0.78rem;">None.</div>';
        return `<div class="cov-list">${files.map(f => {
            const arch = String(f.language_id || '').split(':')[0];
            return `<div class="cov-row"><span class="dim"><i class="fa-regular fa-file"></i></span>
                ${EntityRenderer.renderFileName(f.file_name, f.file_md5, col)}
                <span>${arch ? `<span class="cov-arch">${escapeHtml(arch)}</span>` : ''}</span>
                <span class="dim" style="text-align:right" title="Functions">${Number(f.function_count || 0).toLocaleString()}</span></div>`;
        }).join('')}</div>`;
    },

    /** Circle packing of the shown families: area = files, colour = cohesion. Click opens the cluster. */
    _drawFamilyMap() {
        const el = document.getElementById('cdo-map');
        const ov = this._ov || {};
        const tree = ov.family_map || ov.families || [];
        if (!el || !tree.length || !window.d3) return;
        const W = el.clientWidth || 600, H = el.clientHeight || 320;
        // Loose clusters (no family of their own) are dashed groups around the families inside them.
        const root = d3.pack().size([W, H]).padding(n => (n.depth ? 6 : 3))(
            d3.hierarchy({ children: tree }, d => (d.cid === undefined || d.family === false ? d.children : null))
                .sum(d => (d.cid && d.family !== false ? Math.max(1, d.matching || 0) : 0))
                .sort((x, y) => y.value - x.value));
        // Map labels drop the repeated prefix: 'mirai#Unix.Trojan.Mirai-8041698-0' -> 'Mirai-8041698-0'.
        const brief = name => String(name).split('#').pop().split('.').pop() || String(name);
        const short = (name, r) => { const t = brief(name); const k = Math.max(3, Math.floor(r / 3)); return t.length > k ? t.slice(0, k) + '…' : t; };
        const tip = f => escapeHtml(`${f.cluster_name || `Cluster ${f.cid}`}\n${Number(f.matching || 0).toLocaleString()} files · cohesion ${Number(f.cohesion_score || 0).toFixed(2)}${f.family === false ? '\nLoose group: the families inside are tighter' : ''}`);
        const open = f => `onclick="${escapeAttr(`event.stopPropagation(); openCluster(${jsString(f.cluster_uuid)}, true, event, ${jsString(this._ovAxis())})`)}"`;
        const out = root.descendants().filter(n => n.depth > 0).map(n => {
            const f = n.data, x = n.x.toFixed(1), y = n.y.toFixed(1), r = n.r.toFixed(1);
            const name = f.cluster_name || `Cluster ${f.cid}`;
            if (f.family === false) {
                const lab = n.r > 40 ? `<text x="${x}" y="${(n.y - n.r + 12).toFixed(1)}" text-anchor="middle" class="cdo-map-g">${escapeHtml(short(name, n.r * 1.6))}</text>` : '';
                return `<g class="cdo-map-grp" ${open(f)}><circle cx="${x}" cy="${y}" r="${r}"><title>${tip(f)}</title></circle>${lab}</g>`;
            }
            const lab = n.r > 15 ? `<text x="${x}" y="${(n.y + 3).toFixed(1)}" text-anchor="middle" class="cdo-map-t">${escapeHtml(short(name, n.r))}</text>` : '';
            return `<g class="cdo-map-n" ${open(f)}><circle cx="${x}" cy="${y}" r="${r}" style="--c:${this._cohColor(f.cohesion_score)}"><title>${tip(f)}</title></circle>${lab}</g>`;
        }).join('');
        el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Family map">${out}</svg>`;
    },

    /** Uploads card, switchable to the in-the-wild dates (overview `dates`). */
    _ovUpCard(ov, loading) {
        const dates = ov.dates || { entry_date: ov.uploads };
        const fields = this._OV_DATES.filter(([f]) => dates[f]);
        const field = this._ovDateField && dates[this._ovDateField] ? this._ovDateField : 'all';
        const shown = field === 'all' ? fields : fields.filter(([f]) => f === field);
        const pills = [['all', 'All'], ...fields.map(([f, l]) => [f, l])];
        const seg = fields.length > 1 ? `<div class="cov-tl-seg" role="group" aria-label="Date">${pills.map(([f, l]) => `<button type="button" aria-pressed="${f === field}" onclick="CollectionDetailView._ovDates('${f}')">${l}</button>`).join('')}</div>` : '';
        const title = field === 'all' ? (fields.length > 1 ? 'Files over time' : 'Uploads') : shown[0][1];
        return `<div class="cov-card cdo-wide cdo-r2" id="cdo-up">
            ${this._ovTitle('fa-solid fa-chart-column', 'var(--info, #3b82f6)', title, 'Files per period by upload date, and by the dates they were first and last seen in the wild (only files whose upload gave them). Same time span and same scale in every view.')}
            ${seg}
            ${loading ? '<span class="cdo-sk" style="height:126px;"></span>' : this._uploadsChart(dates, shown, fields)}
        </div>`;
    },

    // [field, label, colour]; in "All" the later series are drawn narrower on top, so equal bars stay visible.
    _OV_DATES: [['entry_date', 'Uploads', 'var(--info, #3b82f6)'], ['first_seen', 'First seen', 'var(--success)'], ['last_seen', 'Last seen', 'var(--warning, #d97706)']],

    _ovDates(field) {
        this._ovDateField = field;
        const el = document.getElementById('cdo-up');
        if (el && this._ov) el.outerHTML = this._ovUpCard(this._ov, false);
    },

    /** Files per bin for each shown date field, over the server's shared bins; y-scale is the max over every field. */
    _uploadsChart(dates, shown, all) {
        const first = all.length ? dates[all[0][0]] : null;
        if (!first || !all.some(([f]) => (dates[f].counts || []).some(Boolean))) return '<div class="dim" style="font-size:0.78rem;">No dates.</div>';
        const day = v => new Date(+v).toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: '2-digit' });
        const max = Math.max(1, ...all.flatMap(([f]) => dates[f].counts));
        const W = [100, 64, 30];
        const bars = first.counts.map((_, i) => {
            const a = first.start + i * first.step;
            const tip = `${day(a)} – ${day(a + first.step)}\n` + shown.map(([f, l]) => `${l}: ${dates[f].counts[i].toLocaleString()}`).join('\n');
            const is = shown.map(([f, , c], k) => {
                const n = dates[f].counts[i];
                const w = shown.length > 1 ? W[k] : 100;
                return n ? `<i style="height:${Math.max(4, n / max * 100)}%; width:${w}%; left:${(100 - w) / 2}%; background:color-mix(in srgb, ${c} 75%, var(--card-bg));"></i>` : '';
            }).join('');
            return `<span title="${escapeAttr(tip)}">${is}</span>`;
        }).join('');
        const leg = shown.length > 1 ? `<div class="cov-mleg">${shown.map(([f, l, c]) => `<span><i style="background:${c}"></i>${l} <b>${dates[f].counts.reduce((s, n) => s + n, 0).toLocaleString()}</b></span>`).join('')}</div>` : '';
        return `<div class="cdo-bars">${bars}</div>
            <div class="cdo-axis"><span>${day(first.start)}</span><span>${day(first.start + first.step * first.counts.length)}</span></div>${leg}`;
    },

    /** Stacked share bar + legend, the cluster Metadata card's look (cov-sbar / cov-mleg). */
    _mixBlock(icon, label, items, total) {
        if (!items.length) return '';
        const colors = ['var(--info, #3b82f6)', 'var(--success)', 'var(--warning, #d97706)', 'var(--token-instruction, #e11d48)', 'var(--accent)', 'var(--dim)'];
        const shown = items.slice(0, 5);
        const rest = Math.max(0, total - shown.reduce((s, x) => s + x.count, 0));
        const parts = [...shown.map((x, i) => [colors[i], x.count, x.value]), ...(rest ? [[colors[5], rest, 'other']] : [])];
        return `<div><div class="cov-h3"><i class="${icon}"></i>${label}</div>
            <div class="cov-sbar">${parts.map(([c, n, v]) => `<span style="flex:${n}; background:${c}" title="${escapeAttr(`${v}: ${n.toLocaleString()}`)}"></span>`).join('')}</div>
            <div class="cov-mleg">${parts.map(([c, n, v]) => `<span title="${escapeAttr(v)}"><i style="background:${c}"></i>${escapeHtml(v)} <b>${total ? Math.round(n / total * 100) : 0}%</b></span>`).join('')}</div></div>`;
    },

    _renderOverview() {
        const ov = this._ov || {};
        const loading = !this._ov;
        if (ov.error) return `<div class="dim" style="font-size:0.8rem; margin-bottom:20px;"><i class="fa-solid fa-triangle-exclamation"></i> Overview unavailable. Refresh to retry.</div>`;
        const col = this._ovCol;
        const fams = ov.families || [];
        const coll = this._ovColl || {};
        const total = Number(coll.total_files || 0);
        const filesUrl = Nav.buildUIUrl(col, ['files']) + this._ovAxisQs('group_axis');
        const ax = this._ovAxis();
        // Cluster cards take the axis colour (BinSimScoreTypes), like their badges.
        const OK = ((window.BinSimScoreTypes || {})[ax === 'overall' ? 'score' : `score_${ax}`] || {}).color || 'var(--success)', INFO = 'var(--info, #3b82f6)', WARN = 'var(--warning, #d97706)';
        const sk = (n, h = 14) => `<div class="cov-list">${Array.from({ length: n }, (_, i) => `<span class="cdo-sk" style="height:${h}px; width:${92 - (i * 17) % 40}%"></span>`).join('')}</div>`;
        const none = text => `<div class="dim" style="font-size:0.78rem;">${text}</div>`;
        const noClusters = none('No file clusters yet. Build them from the Clusters tab.');
        const hidden = ov.families_hidden ? `<span class="dim" style="font-size:0.72rem;">${Number(ov.families_hidden).toLocaleString()} small or packed not shown</span>` : '';
        const big = v => (loading ? undefined : v);

        const mapCard = `<div class="cov-card cdo-wide cdo-r1">
            ${this._ovTitle('fa-solid fa-diagram-project', OK, 'Family map', 'File families: circle area is the number of files, colour is cohesion (green = near-identical files). A dashed circle is a loose cluster that groups several families. Click to open a cluster.', big(ov.families_total))}
            <div id="cdo-map" class="cdo-map">${loading ? '<span class="cdo-sk cdo-sk-map"></span>' : fams.length ? '' : noClusters}</div>
            <div class="cov-foot">${this._ovBtn('fa-solid fa-bullseye', OK, 'Clusters', Nav.buildUIUrl(col, ['files', 'clusters']) + this._ovAxisQs())}${hidden}</div>
        </div>`;
        const top = fams.slice(0, 7);
        const max = Math.max(1, ...top.map(f => f.matching || 0));
        const famCard = `<div class="cov-card cdo-r1">
            ${this._ovTitle('fa-solid fa-bullseye', OK, 'Families', 'Largest file clusters. A name opens the cluster; the layers icon opens its N-way diff.')}
            ${loading ? sk(7, 18) : top.length ? `<div class="cov-list">${top.map(f => this._ovFamRow(f, max)).join('')}</div>` : noClusters}
            <div class="cov-foot">${this._ovBtn('fa-solid fa-file-code', OK, 'Files by cluster', filesUrl)}</div>
        </div>`;
        const recentCard = `<div class="cov-card cdo-r2">
            ${this._ovTitle('fa-solid fa-clock-rotate-left', INFO, 'Recent files', 'Last files added to this collection.', total)}
            ${loading ? sk(8) : this._ovFileRows(ov.recent || [])}
            <div class="cov-foot">${this._ovBtn('fa-solid fa-file-code', INFO, 'Files', filesUrl)}</div>
        </div>`;
        const looseCard = `<div class="cov-card cdo-r2">
            ${this._ovTitle('fa-regular fa-circle', WARN, 'No cluster', 'Files that joined no cluster: the rare ones, worth a look first.', big(ov.unclustered_total))}
            ${loading ? sk(8) : ov.has_clusters ? this._ovFileRows(ov.rare || []) : noClusters}
            <div class="cov-foot">${this._ovBtn('fa-solid fa-file-code', WARN, 'Files', filesUrl, 'The unclustered files are the last group')}</div>
        </div>`;
        const upCard = this._ovUpCard(ov, loading);
        const day = ms => new Date(+ms).toISOString().slice(0, 10);
        const days = ov.active_days || 30;
        const byActivity = sort => this._ovBtn('fa-solid fa-table-list', OK, 'All clusters', `${Nav.buildUIUrl(col, ['files', 'clusters'])}${this._ovAxisQs() || '?'}${this._ovAxisQs() ? '&' : ''}sort_by=${sort}${sort === 'active' && days !== 30 ? `&active_days=${days}` : ''}`, 'The clusters table, sorted the same way');
        const untimed = Number(ov.families_untimed || 0);
        const noDates = untimed ? none(`${untimed.toLocaleString()} famil${untimed > 1 ? 'ies have' : 'y has'} no member dates yet: rebuild the clusters or run <code>bsimvis metadata backfill-timeline -c ${escapeHtml(col)}</code>.`) : '';
        const activeCard = `<div class="cov-card cdo-r2">
            ${this._ovTitle('fa-solid fa-fire', OK, `Most active <span class="dim" style="font-weight:400; font-size:0.75rem;">${Number(days)} d</span>`, `Families with the most members uploaded in the last ${days} days.`)}
            ${loading ? sk(7, 18) : (ov.active || []).length ? `<div class="cov-list">${ov.active.map(f => this._ovActRow(f, `<b style="color:var(--success)">+${Number(f.new_members).toLocaleString()}</b> <span class="dim">/ ${Number(f.matching || 0).toLocaleString()}</span>`, `New members in ${days} days / files`)).join('')}</div>` : (noDates || (fams.length ? none(`No family grew in the last ${days} days.`) : noClusters))}
            <div class="cov-foot">${byActivity('active')}</div>
        </div>`;
        const seenCard = `<div class="cov-card cdo-r2">
            ${this._ovTitle('fa-solid fa-satellite-dish', OK, 'Active in the wild', 'Families by the last time one of their members was seen in the wild (first_seen / last_seen, when the upload gave them).')}
            ${loading ? sk(7, 18) : (ov.seen || []).length ? `<div class="cov-list">${ov.seen.map(f => this._ovActRow(f, `<span class="dim">${day(f.last_seen)}</span>`, `Last seen ${day(f.last_seen)}, first seen ${day(f.first_seen)}`)).join('')}</div>` : (noDates || (fams.length ? none('No family member has a first or last seen date.') : noClusters))}
            <div class="cov-foot">${byActivity('last_seen')}</div>
        </div>`;
        const changedCard = `<div class="cov-card cdo-r2">
            ${this._ovTitle('fa-solid fa-clock-rotate-left', OK, 'Recently changed', 'Families by the upload date of their newest member: the last ones to grow.')}
            ${loading ? sk(7, 18) : (ov.changed || []).length ? `<div class="cov-list">${ov.changed.map(f => this._ovActRow(f, `<span class="dim">${day(f.last_added)}</span>`, `Newest member uploaded on ${day(f.last_added)}, oldest on ${day(f.first_added)}`)).join('')}</div>` : (noDates || noClusters)}
            <div class="cov-foot">${byActivity('last_added')}</div>
        </div>`;
        // filetype strings can be long ("ELF 32-bit LSB executable, ARM, ..."): group on the format word.
        const fmt = {};
        (ov.filetype || []).forEach(x => { const k = String(x.value).split(/[\s,]/)[0] || 'unknown'; fmt[k] = (fmt[k] || 0) + x.count; });
        const fmtItems = Object.entries(fmt).map(([value, count]) => ({ value, count })).sort((a, b) => b.count - a.count);
        const arch = {};
        (ov.arch || []).forEach(x => { const k = String(x.value).split(':')[0]; arch[k] = (arch[k] || 0) + x.count; });
        const archItems = Object.entries(arch).map(([value, count]) => ({ value, count })).sort((a, b) => b.count - a.count);
        const tags = (ov.tags || []).slice(0, 10);
        const mixBody = loading ? sk(6, 16)
            : (archItems.length || fmtItems.length || tags.length) ? `${this._mixBlock('fa-solid fa-microchip', 'Architecture', archItems, total)}
            ${this._mixBlock('fa-solid fa-file-code', 'Format', fmtItems, total)}
            ${tags.length ? `<div><div class="cov-h3"><i class="fa-solid fa-tags"></i>Top tags</div><div class="cov-chips">${tags.map(t => `<span class="cov-chip" style="--c:${escapeAttr(window.TagColor ? TagColor.forTag(t.tag) : 'var(--accent)')}" title="${escapeAttr(t.tag)}">${escapeHtml(t.tag)} <span class="dim">${Number(t.count).toLocaleString()}</span></span>`).join('')}</div></div>` : ''}`
            : none('No architecture, format or tag indexed yet.');
        const mixCard = `<div class="cov-card cdo-r1">
            ${this._ovTitle('fa-solid fa-chart-pie', INFO, 'Mix', 'How the files split by architecture and format, and the tags most of them carry.')}
            ${mixBody}
        </div>`;
        return `${this._ovAxisSeg()}<div class="cdo-grid${loading ? ' cdo-loading' : ''}" aria-busy="${loading}">${mapCard}${famCard}${mixCard}${recentCard}${looseCard}${upCard}<div class="cdo-row3">${activeCard}${changedCard}${seenCard}</div></div>`;
    },

    _OV_CSS: `
.cdo-bar { display:flex; align-items:center; gap:8px; margin-bottom:10px; font-size:0.78rem; }
.cdo-fam { display:grid; grid-template-columns:auto minmax(0,1fr) 64px 22px; gap:8px; align-items:center; padding:3px 4px; border-radius:5px; font-size:0.8rem; }
.cdo-fam:hover { background:var(--hover); }
.cdo-act { display:grid; grid-template-columns:auto minmax(0,1fr) auto; gap:8px; align-items:center; padding:3px 4px; border-radius:5px; font-size:0.8rem; }
.cdo-act:hover { background:var(--hover); }
.cdo-act > a.fb-link { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; cursor:pointer; }
.cdo-fam > a.fb-link { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; cursor:pointer; }
.cdo-size { height:6px; border-radius:3px; background:var(--hover); overflow:hidden; }
.cdo-size i { display:block; height:100%; background:color-mix(in srgb, var(--success) 70%, var(--card-bg)); }
.cdo-go { color:var(--dim); cursor:pointer; text-align:center; }
.cdo-go:hover { color:var(--accent); }
#coll-overview .cov-btn { min-width:0; padding:3px 9px; }
#coll-overview .cov-foot { align-items:center; }
.cdo-map { height:320px; display:flex; align-items:center; justify-content:center; }
.cdo-map > svg { width:100%; height:100%; }
.cdo-map-n { cursor:pointer; }
.cdo-map-n circle { fill:color-mix(in srgb, var(--c) 22%, var(--card-bg)); stroke:color-mix(in srgb, var(--c) 70%, transparent); stroke-width:1.2; }
.cdo-map-n:hover circle { fill:color-mix(in srgb, var(--c) 40%, var(--card-bg)); stroke:var(--c); }
.cdo-map-t { fill:var(--text); font-size:9px; pointer-events:none; }
.cdo-map-grp { cursor:pointer; }
.cdo-map-grp circle { fill:color-mix(in srgb, var(--dim) 6%, transparent); stroke:var(--dim); stroke-width:1; stroke-dasharray:4 3; }
.cdo-map-grp:hover circle { stroke:var(--text); }
.cdo-map-g { fill:var(--dim); font-size:10px; pointer-events:none; }
.cdo-bars { display:flex; align-items:flex-end; gap:2px; height:110px; }
.cdo-bars > span { flex:1; height:100%; position:relative; border-radius:2px; }
.cdo-bars > span:hover { background:var(--hover); }
.cdo-bars i { position:absolute; bottom:0; display:block; border-radius:2px 2px 0 0; }
.cdo-axis { display:flex; justify-content:space-between; font-size:0.68rem; color:var(--dim); }
.cdo-grid { display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:12px; margin-bottom:20px; }
.cdo-wide { grid-column:span 2; }
.cdo-row3 { grid-column:1 / -1; display:grid; grid-template-columns:repeat(auto-fit, minmax(320px, 1fr)); gap:12px; }
.cdo-r1 { min-height:420px; }
.cdo-r2 { min-height:300px; }
.cdo-sk { display:block; border-radius:4px; margin:3px 0; background:linear-gradient(90deg, var(--hover) 25%, color-mix(in srgb, var(--hover) 45%, var(--card-bg)) 50%, var(--hover) 75%); background-size:200% 100%; animation:cdo-sh 1.3s ease-in-out infinite; }
.cdo-sk-map { width:260px; height:260px; border-radius:50%; }
@keyframes cdo-sh { from { background-position:100% 0; } to { background-position:-100% 0; } }
@media (prefers-reduced-motion: reduce) { .cdo-sk { animation:none; } }
@media (max-width:1300px) { .cdo-grid { grid-template-columns:repeat(2, minmax(0,1fr)); } }
@media (max-width:700px) { .cdo-grid { grid-template-columns:minmax(0,1fr); } .cdo-wide { grid-column:auto; } .cdo-r1, .cdo-r2 { min-height:0; } }
`,

    _statusBadge(status) {
        if (status === 'current') return `<span style="background:rgba(16,185,129,0.15); border:1px solid rgba(16,185,129,0.35); color:#10b981; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle-check"></i> Current</span>`;
        if (status === 'outdated') return `<span style="background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.35); color:#f59e0b; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle-exclamation"></i> Outdated</span>`;
        return `<span style="background:rgba(156,163,175,0.15); border:1px solid rgba(156,163,175,0.3); color:#9ca3af; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle"></i> ${status || 'created'}</span>`;
    },

    _paramRow(key, label, hint, p, step) {
        const p2 = p || {};
        const value = p2.value !== undefined ? p2.value : '';
        const origin = p2.locked
            ? `locked on this collection`
            : `not set — falls back to the global default`;
        return `
        <div style="padding:10px 0; border-bottom:1px solid var(--border);">
            <div style="display:flex; justify-content:space-between; align-items:center; gap:12px;">
                <div>
                    <div style="font-size:0.82rem; color:var(--text); font-weight:600;">${label}</div>
                    <div style="font-size:0.72rem; color:var(--dim); margin-top:2px;">${hint}</div>
                </div>
                <input id="coll-param-${key}" type="number" step="${step}" value="${value}"
                    style="width:110px; padding:6px 10px; background:var(--hover); border:1px solid var(--border); border-radius:6px; color:var(--accent); font-family:monospace; font-weight:600; font-size:0.85rem; text-align:right;">
            </div>
            <div style="font-size:0.7rem; color:var(--dim); margin-top:6px;">${origin} · global default <code>${p2.default !== undefined ? p2.default : '—'}</code></div>
        </div>`;
    },

    _algoRow(p) {
        const value = String(p.value || '');
        const options = window.SimAlgos
            ? window.SimAlgos.optionsHtml(value, { buildable: true })
            : `<option value="${escapeAttr(value)}">${escapeHtml(value)}</option>`;
        return `
        <div style="padding:10px 0; border-bottom:1px solid var(--border);">
            <div style="display:flex; justify-content:space-between; align-items:center; gap:12px;">
                <div>
                    <div style="font-size:0.82rem; color:var(--text); font-weight:600;">Algorithm</div>
                    <div style="font-size:0.72rem; color:var(--dim); margin-top:2px;">Function similarity scoring algorithm.</div>
                </div>
                <select id="coll-param-algo" style="max-width:220px; padding:6px 10px; background:var(--hover); border:1px solid var(--border); border-radius:6px; color:var(--accent); font-family:monospace; font-weight:600; font-size:0.85rem;">${options}</select>
            </div>
            <div style="font-size:0.7rem; color:var(--dim); margin-top:6px;">${p.locked ? 'locked on this collection' : 'not set — falls back to the global default'} · global default <code>${escapeHtml(String(p.default ?? '—'))}</code></div>
        </div>`;
    },

    _renderParamsCard(name, params) {
        return `
        <div>
            <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); margin-bottom:12px; display:flex; align-items:center; gap:7px;">
                <i class="fa-solid fa-sliders"></i> Similarity Parameters
            </div>
            <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:18px;">
                ${this._paramRow('min_features', 'Min Features', 'Functions below this many features skip the BSim vector and match by exact FunctionID hash only.', params.min_features, '1')}
                ${this._paramRow('min_score', 'Min Score', 'Similarity floor — pairs scoring below this are not stored.', params.min_score, '0.01')}
                ${this._algoRow(params.algo || {})}
                <div style="display:flex; align-items:center; gap:8px; margin-top:14px; padding:10px 12px; background:rgba(245,158,11,0.1); border:1px solid rgba(245,158,11,0.3); border-radius:6px; font-size:0.75rem; color:#f59e0b;">
                    <i class="fa-solid fa-triangle-exclamation"></i>
                    <span>These are locked on the first similarity build. Changing them does not rescore existing edges — rebuild the collection to apply.</span>
                </div>
                <div style="display:flex; gap:10px; margin-top:14px; flex-wrap:wrap;">
                    <button onclick="window.collectionDetailSaveParams(${escapeAttr(jsString(name))}, this)" class="ui-button hdr-btn primary"><i class="fa-solid fa-floppy-disk"></i> Save</button>
                    <button onclick="window.collectionDetailRebuildSim(${escapeAttr(jsString(name))}, this)" class="ui-button hdr-btn"><i class="fa-solid fa-rotate-right"></i> Rebuild Similarity</button>
                </div>
            </div>
        </div>`;
    },

    _renderPage(coll, pools, collectionActiveJobs = [], params = {}) {
        const name = coll.name;
        const files = coll.total_files !== undefined ? coll.total_files : 0;
        const funcs = coll.total_functions !== undefined ? coll.total_functions : 0;
        const batches = coll.total_batches !== undefined ? coll.total_batches : 0;
        const counts = coll.counts || {};
        const num = v => (v === undefined ? '—' : Number(v).toLocaleString());
        const updated = coll.last_updated ? (typeof window.formatDate === 'function' ? formatDate(toEpochMs(coll.last_updated)) : new Date(toEpochMs(coll.last_updated)).toLocaleString()) : '—';

        const filesUrl = `/collections/${encodeURIComponent(name)}/files`;
        const funcsUrl = `/collections/${encodeURIComponent(name)}/functions`;
        const batchesUrl = `/collections/${encodeURIComponent(name)}/batches`;

        const poolsHtml = `
        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
            <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border); background: var(--hover); color:var(--dim);">
                        <th style="padding:10px 15px;">Pool</th>
                        <th style="padding:10px 15px;">Status</th>
                        <th style="padding:10px 15px; text-align:right;">Last Built</th>
                    </tr>
                </thead>
                <tbody>
                    ${pools.map(pool => {
                        const poolUrl = `/pools/${encodeURIComponent(pool.id)}`;
                        const lastBuilt = pool.last_built_at && pool.last_built_at != '0' ? (typeof window.formatDate === 'function' ? formatDate(parseInt(pool.last_built_at)) : pool.last_built_at) : '—';
                        return `
                        <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                            <td style="padding:10px 15px;"><a href="${poolUrl}" onclick="Nav.openPath(${escapeAttr(jsString(poolUrl))}, event)" style="font-weight:bold; color:var(--text); text-decoration:none;">${pool.name || pool.id}</a></td>
                            <td style="padding:10px 15px;">${this._statusBadge(pool.sync_status || 'created')}</td>
                            <td style="padding:10px 15px; text-align:right; color:var(--dim);">${lastBuilt}</td>
                        </tr>`;
                    }).join('')}
                </tbody>
            </table>
        </div>`;

        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:22px;">

            <!-- HEADER -->
            <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap;">
                <div>
                    <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px; flex-wrap:wrap;">
                        <i class="fa-solid fa-layer-group" style="font-size:1.4rem; color:var(--accent);"></i>
                        <h1 style="margin:0; font-size:1.5rem; color:var(--text);">${name}</h1>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                        <code style="font-size:0.8rem; color:var(--dim); background: var(--hover); padding:3px 10px; border-radius:4px; border:1px solid var(--border);">${name}</code>
                        <button onclick="copyToClipboard(${escapeAttr(jsString(name))}, this)" class="btn-copy" title="Copy Collection Name" style="padding:4px 8px; font-size:0.75rem;">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </div>
                <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                    ${UI.Button.render({ className: 'hdr-btn primary', icon: 'fa-solid fa-upload', label: 'Upload', onClick: `Nav.openPath('/collections/${encodeURIComponent(name)}/upload')` })}
                    ${UI.Button.render({ className: 'hdr-btn', icon: 'fa-solid fa-robot', label: 'Analyze', onClick: `openFileAnalysisModal({ collection: ${jsString(name)} })` })}
                    ${UI.MoreMenu.render([
                        { label: 'Maintenance…', icon: 'fa-solid fa-screwdriver-wrench', onClick: `window.openMaintenanceWindow(${jsString(name)})` },
                        { label: 'Clean', icon: 'fa-solid fa-broom', onClick: `window.collectionDetailClean(${jsString(name)}, this)` },
                        '-',
                        { label: 'Delete collection', icon: 'fa-solid fa-trash-can', danger: true, onClick: `window.collectionDetailDelete(${jsString(name)}, this)` },
                    ])}
                </div>
            </div>

            <!-- META ROW -->
            <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:center;">
                <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px 18px; font-size:0.8rem; display:flex; align-items:center; gap:8px;">
                    <i class="fa-solid fa-clock-rotate-left" style="color:var(--dim);"></i>
                    <span style="color:var(--dim);">Last Updated</span>
                    <span style="color:var(--text); font-weight:600;">${updated}</span>
                </div>
            </div>

            <div id="coll-overview">${this._ovSkeleton(name, coll)}</div>

            <!-- MAIN GRID -->
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:20px; align-items:start;">

                <!-- LEFT COLUMN -->
                <div style="display:flex; flex-direction:column; gap:20px;">
                    <div>
                        <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); margin-bottom:12px; display:flex; align-items:center; gap:7px;">
                            <i class="fa-solid fa-circle-info"></i> Collection Metadata
                        </div>
                        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                            <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                                <thead>
                                    <tr style="border-bottom:1px solid var(--border); background: var(--hover); color:var(--dim);">
                                        <th style="padding:10px 15px;">Entity Type</th>
                                        <th style="padding:10px 15px; text-align:right;">Count</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-boxes-stacked" style="margin-right:8px; width:16px;"></i>Batches</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="${batchesUrl}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${batches}</a></td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-file-code" style="margin-right:8px; width:16px;"></i>Files</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="${filesUrl}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${files}</a></td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-right-left" style="margin-right:8px; width:16px;"></i>Similarities</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/files/similarities" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${num(counts.file_sims)}</a></td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-bullseye" style="margin-right:8px; width:16px;"></i>Clusters</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/files/clusters" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${num(counts.file_clusters)}</a></td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-code" style="margin-right:8px; width:16px;"></i>Functions</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="${funcsUrl}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${funcs}</a></td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-fingerprint" style="margin-right:8px; width:16px;"></i>Features</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/features" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${num(counts.features)}</a></td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-tags" style="margin-right:8px; width:16px;"></i>Tags</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/tags" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">Manage</a></td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-code-compare" style="margin-right:8px; width:16px;"></i>Function Similarities</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/functions/similarities" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${num(counts.function_sims)}</a></td>
                                    </tr>
                                    <tr style="transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-bullseye" style="margin-right:8px; width:16px;"></i>Function Clusters</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/functions/clusters" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${num(counts.function_clusters)}</a></td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- RIGHT COLUMN -->
                <div style="display:flex; flex-direction:column; gap:20px;">
                    ${this._renderParamsCard(name, params)}
                    <div>
                        <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); margin-bottom:12px; display:flex; align-items:center; gap:7px;">
                            <i class="fa-solid fa-diagram-project"></i> Associated Pools
                        </div>
                        ${pools.length > 0 ? poolsHtml : '<div style="color:var(--dim); font-size:0.85rem; padding:20px; text-align:center; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">Not associated with any pools</div>'}
                    </div>
                </div>

            </div>

            <!-- ACTIVE / RUNNING JOBS -->
            <div id="active-jobs-container">
                ${this._renderActiveJobsSection(name, collectionActiveJobs)}
            </div>

        </div>`;
    },

    _renderActiveJobsSection(name, collectionActiveJobs) {
        return `
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
                    <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); display:flex; align-items:center; gap:7px;">
                        <i class="fa-solid fa-server"></i> Active Jobs
                    </div>
                    <button onclick="Nav.openPath('/collections/${encodeURIComponent(name)}/jobs')" style="background: var(--hover); border:1px solid var(--border); color:var(--text); padding:5px 12px; border-radius:4px; font-size:0.75rem; font-weight:600; cursor:pointer; display:inline-flex; align-items:center; gap:6px; transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='var(--border)'"><i class="fa-solid fa-server"></i> See All Jobs</button>
                </div>
                ${collectionActiveJobs.length > 0 ? `
                <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                    <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                        <thead>
                            <tr style="border-bottom:1px solid var(--border); background: var(--hover); color:var(--dim);">
                                <th style="padding:10px 15px;">Job ID</th>
                                <th style="padding:10px 15px;">Type</th>
                                <th style="padding:10px 15px;">Status</th>
                                <th style="padding:10px 15px;">Progress</th>
                                <th style="padding:10px 15px;">Duration</th>
                                <th style="padding:10px 15px; text-align:right;">Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${collectionActiveJobs.map(job => {
                                const jobUrl = `/collections/${encodeURIComponent(name)}/jobs`;
                                const status = job.status;
                                let actions = '<div class="job-actions" style="display:flex; justify-content:flex-end;">';
                                if (status === 'pending' || status === 'running') {
                                    actions += `<button class="job-btn-action danger" onclick="cancelJob(${escapeAttr(jsString(job.id))})" title="Cancel Job"><i class="fa-solid fa-ban"></i></button>`;
                                }
                                if (status === 'failed' || status === 'cancelled' || status === 'completed') {
                                    actions += `<button class="job-btn-action" onclick="retryJob(${escapeAttr(jsString(job.id))})" title="Retry/Resume Job"><i class="fa-solid fa-rotate-right"></i></button>`;
                                }
                                actions += `<button class="job-btn-action info" onclick="showJobDetails(${escapeAttr(jsString(job.id))})" title="View Logs & Details"><i class="fa-solid fa-circle-info"></i></button>`;
                                actions += '</div>';

                                let progressClass = '';
                                if (status === 'running') progressClass = 'progress-running';
                                if (status === 'completed') progressClass = 'progress-completed';
                                if (status === 'failed' || status === 'cancelled') progressClass = 'progress-failed';

                                const progressHtml = `
                                    <div class="job-progress-container">
                                        <div class="job-progress-track">
                                            <div class="job-progress-fill ${progressClass}" style="width: ${job.progress}%"></div>
                                        </div>
                                        <span class="job-progress-text">${job.progress}%</span>
                                    </div>
                                `;

                                let statusIcon = 'fa-circle-notch fa-spin';
                                if (status === 'completed') statusIcon = 'fa-check-circle';
                                if (status === 'failed') statusIcon = 'fa-exclamation-circle';
                                if (status === 'cancelled') statusIcon = 'fa-ban';
                                if (status === 'pending') statusIcon = 'fa-clock';

                                const statusBadge = `<span class="job-status-badge status-${status}"><i class="fa-solid ${statusIcon}"></i> ${status.toUpperCase()}</span>`;

                                const durationHtml = window.formatDuration ? window.formatDuration(job.created_at, job.updated_at, status) : '-';

                                return `
                                <tr style="border-bottom: 1px solid var(--border); transition:background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='transparent'">
                                    <td style="padding:10px 15px; font-family:monospace;"><a href="${jobUrl}" onclick="Nav.openPath(${escapeAttr(jsString(jobUrl))}, event)" style="color:var(--accent); text-decoration:none; font-weight:600;">${job.id}</a></td>
                                    <td style="padding:10px 15px; font-weight:600; text-transform:capitalize;">${job.type}</td>
                                    <td style="padding:10px 15px;">${statusBadge}</td>
                                    <td style="padding:10px 15px;">${progressHtml}</td>
                                    <td style="padding:10px 15px;">${durationHtml}</td>
                                    <td style="padding:10px 15px; text-align:right;">${actions}</td>
                                </tr>`;
                            }).join('')}
                        </tbody>
                    </table>
                </div>` : `
                <div style="color:var(--dim); font-size:0.85rem; padding:20px; text-align:center; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">
                    No active jobs running for this collection.
                </div>
                `}`;
    }
};

window.collectionDetailSaveParams = async function(collName, btn) {
    const minFeatures = document.getElementById('coll-param-min_features');
    const minScore = document.getElementById('coll-param-min_score');
    const algo = document.getElementById('coll-param-algo');
    const body = { collection: collName };
    if (minFeatures && minFeatures.value !== '') body.min_features = Number(minFeatures.value);
    if (minScore && minScore.value !== '') body.min_score = Number(minScore.value);
    if (algo && algo.value !== '') body.algo = algo.value;

    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch('/api/collection/params', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
        alert(`Saved: ${Object.entries(data.updated).map(([k, v]) => `${k}=${v}`).join(', ')}\n\nExisting similarity edges keep their old scores until you rebuild.`);
        Nav.openPath(window.location.pathname);
    } catch(e) {
        alert(`Failed to save parameters: ${e.message}`);
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-floppy-disk"></i> Save'; }
    }
};

window.collectionDetailRebuildSim = async function(collName, btn) {
    if (!confirm(`Clear and rebuild every function similarity in "${collName}" with the current parameters? This can take a long time on a large collection.`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch('/api/similarity/rebuild', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: collName, all: true })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
        alert(`Similarity rebuild enqueued! Job ID: ${data.job_id}`);
    } catch(e) {
        alert(`Failed to enqueue rebuild: ${e.message}`);
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-rotate-right"></i> Rebuild Similarity'; }
    }
};

window.collectionDetailClean = async function(collName, btn) {
    if (!confirm(`Clean up temporary upload keys for collection "${collName}"?`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch(`/api/collection/clean`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: collName })
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        alert(`Collection clean enqueued! Job ID: ${data.job_id}`);
    } catch(e) {
        alert(`Failed to clean collection: ${e.message}`);
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-broom"></i> Clean'; }
    }
};

window.collectionDetailDelete = async function(collName, btn) {
    if (!confirm(`Delete collection "${collName}"? This cannot be undone.`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch(`/api/collection/delete`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: collName })
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        alert(`Collection deletion enqueued! Job ID: ${data.job_id}`);
        Nav.openPath('/collections');
    } catch(e) {
        alert(`Failed to delete collection: ${e.message}`);
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-trash-can"></i> Delete'; }
    }
};

window.openMaintenanceWindow = function(collection) {
    document.getElementById('maintenance-window')?.remove();
    const overlay = document.createElement('div'); overlay.id = 'maintenance-window'; overlay.className = 'maintenance-overlay';
    overlay.innerHTML = `<section class="maintenance-window" role="dialog" aria-modal="true"><header class="maintenance-header"><div><div class="maintenance-kicker">Generated data</div><h2>Collection maintenance</h2><p>${escapeHtml(collection)}</p></div><button class="maintenance-close" onclick="document.getElementById('maintenance-window')?.remove()">&times;</button></header><div class="maintenance-body"><label>Targets<select id="maintenance-target" multiple><option value="function_similarity">Function similarities</option><option value="function_cluster" selected>Function clusters</option><option value="binary_similarity">Binary similarities</option><option value="binary_cluster" selected>Binary clusters</option></select></label><div class="maintenance-grid"><label>Batch UUID<input id="maintenance-batch"></label><label>MD5<input id="maintenance-md5"></label><label>Function address<input id="maintenance-address"></label></div><p class="maintenance-help">Select one or more targets with Ctrl or Cmd-click. Filters are combined; address applies only to function targets.</p></div><footer class="maintenance-footer"><button class="maintenance-btn danger" onclick="window.collectionMaintenance(${escapeAttr(jsString(collection))}, 'clear')">Clear</button><button class="maintenance-btn" onclick="window.collectionMaintenance(${escapeAttr(jsString(collection))}, 'build')">Build</button><button class="maintenance-btn" onclick="window.collectionMaintenance(${escapeAttr(jsString(collection))}, 'resplit')">Resplit</button><button class="maintenance-btn primary" onclick="window.collectionMaintenance(${escapeAttr(jsString(collection))}, 'rebuild')">Rebuild</button></footer></section>`;
    overlay.onclick = event => { if (event.target === overlay) overlay.remove(); }; document.body.appendChild(overlay);
};
window.collectionMaintenance = async function(collection, operation) {
    const targets = [...document.getElementById('maintenance-target').selectedOptions].map(option => option.value);
    const body = { collection, operation, targets };
    for (const field of ['batch', 'md5', 'address']) {
        const value = document.getElementById(`maintenance-${field}`)?.value.trim();
        if (value) body[field] = value;
    }
    if (body.address && targets.some(target => target.startsWith('binary_'))) return alert('Address only applies to function targets.');
    if (['clear', 'rebuild'].includes(operation) && !confirm(`${operation} selected generated data?`)) return;
    try {
        const res = await fetch('/api/maintenance', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
        alert(`Maintenance job enqueued: ${data.job_id}`);
    } catch (error) { alert(`Failed to enqueue maintenance: ${error.message}`); }
};
