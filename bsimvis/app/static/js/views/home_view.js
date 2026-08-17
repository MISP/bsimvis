/**
 * home_view.js
 * Instance-wide homepage: what BSimVis does, where to go, and what is
 * currently in this instance. Cheap counters render first; the heavier
 * insight panels (tags, clusters, batches) load after and are cached
 * server-side for 120s.
 *
 * Icons are intentionally uncoloured (--dim) — the only colour on this page
 * carries meaning (job status, progress). Shared classes live in dashboard.css
 * under "Homepage".
 */

window.HomeView = (function () {
    const CAPABILITIES = [
        {
            icon: 'fa-cloud-arrow-up', title: 'Upload & analyse',
            blurb: 'Push binaries in; Ghidra decompiles them and extracts BSim feature vectors per function. Uploads are grouped into batches you can track and re-run.',
            url: uploadUrl
        },
        {
            icon: 'fa-file-code', title: 'Browse files',
            blurb: 'Every ingested binary with its metadata, tags, function count and cluster memberships. Filter on any indexed field.',
            url: (c) => c ? `/collections/${encodeURIComponent(c)}/files` : '/collections'
        },
        {
            icon: 'fa-code', title: 'Browse functions',
            blurb: 'Decompiled functions with their code, call graph, features and notes. The unit BSim actually compares.',
            url: (c) => c ? `/collections/${encodeURIComponent(c)}/functions` : '/collections'
        },
        {
            icon: 'fa-code-compare', title: 'Similarity',
            blurb: 'Cosine similarity over BSim feature vectors, at function level and whole-binary level. Scored pairs are precomputed by background jobs, then browsable and diffable side by side.',
            url: (c) => c ? `/collections/${encodeURIComponent(c)}/functions/similarities` : '/collections'
        },
        {
            icon: 'fa-bullseye', title: 'Clusters',
            blurb: 'HDBSCAN over the similarity graph groups near-identical functions or binaries. A cluster is usually one library routine, one compiler variant, or one malware family.',
            url: (c) => c ? `/collections/${encodeURIComponent(c)}/files/clusters` : '/collections'
        },
        {
            icon: 'fa-diagram-project', title: 'Pools',
            blurb: 'A pool is a named union of collections with its own similarity and cluster namespace. Set only_cross_collection to keep just the matches that cross a collection boundary — that is how you compare a corpus against a reference set.',
            url: () => '/pools'
        },
        {
            icon: 'fa-tags', title: 'Tags',
            blurb: 'Static tags come from analysis (capa, YARA, library identification); user tags are yours. Both are indexed, so any tag is a filter on any view.',
            url: (c) => c ? `/collections/${encodeURIComponent(c)}/files` : '/collections'
        },
        {
            icon: 'fa-server', title: 'Jobs',
            blurb: 'Ingestion, similarity builds and clustering all run as queued jobs on a worker fleet. Watch progress, pause the queue, retry failures.',
            url: () => '/jobs'
        }
    ];

    let container = null;

    /** Upload lands in the last collection you looked at, else picks one there. */
    function uploadUrl(c) {
        return c ? `/collections/${encodeURIComponent(c)}/upload` : '/upload';
    }

    function esc(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    }

    function num(n) {
        return (n || 0).toLocaleString();
    }

    function ago(ts) {
        if (!ts) return '—';
        if (typeof formatRelativeTime === 'function') return formatRelativeTime(Number(ts));
        return new Date(Number(ts)).toLocaleString();
    }

    function tip(text) {
        return `<span class="home-tip" tabindex="0" data-tip="${esc(text)}"><i class="fa-solid fa-circle-info"></i></span>`;
    }

    function card(inner, extra = '') {
        return `<div class="home-card" style="${extra}">${inner}</div>`;
    }

    function sectionTitle(icon, text, right = '') {
        return `<div style="display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:10px;">
            <h3 style="margin:0; font-size:0.9rem; color:var(--text);"><i class="fa-solid ${icon}" style="color:var(--dim); margin-right:6px;"></i>${text}</h3>
            <div style="font-size:0.72rem; color:var(--dim);">${right}</div></div>`;
    }

    /** Wraps rows in the shared homepage table chrome. */
    function table(headers, rows, empty) {
        if (!rows.length) return `<div style="color:var(--dim); font-size:0.78rem; padding:4px 0;">${empty}</div>`;
        return `<div style="border:1px solid var(--border); border-radius:6px; overflow:hidden;">
            <table class="home-table"><thead><tr>${headers.map(h =>
                `<th${h.align === 'right' ? ' class="num"' : ''}>${h.label}</th>`).join('')}</tr></thead>
            <tbody>${rows.join('')}</tbody></table></div>`;
    }

    function statusBadge(status) {
        const s = String(status || 'pending').toLowerCase();
        const icons = {
            completed: 'fa-check-circle', failed: 'fa-exclamation-circle',
            cancelled: 'fa-ban', pending: 'fa-clock', running: 'fa-circle-notch fa-spin'
        };
        return `<span class="job-status-badge status-${esc(s)}"><i class="fa-solid ${icons[s] || 'fa-circle-notch fa-spin'}"></i> ${esc(s.toUpperCase())}</span>`;
    }

    function progressBar(job) {
        const s = String(job.status || '').toLowerCase();
        const pct = Math.max(0, Math.min(100, Number(job.progress) || 0));
        const cls = s === 'completed' ? 'progress-completed'
            : (s === 'failed' || s === 'cancelled') ? 'progress-failed'
                : s === 'running' ? 'progress-running' : '';
        return `<div class="job-progress-container">
            <div class="job-progress-track"><div class="job-progress-fill ${cls}" style="width:${pct}%"></div></div>
            <span class="job-progress-text">${pct}%</span></div>`;
    }

    function shell(lastCollection) {
        return `
        <div style="flex:1; overflow-y:auto; padding:24px 28px 40px;">
            <div style="max-width:1280px; margin:0 auto;">

                <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap; margin-bottom:18px;">
                    <div>
                        <div style="display:flex; align-items:center; gap:18px; margin-bottom:8px;">
                            <img src="/logo.svg" alt="" style="height:64px;">
                            <h1 style="margin:0; font-size:2.6rem; font-weight:700; letter-spacing:-0.02em;">BSimVis</h1>
                        </div>
                        <p style="color:var(--dim); margin:0; font-size:0.9rem; max-width:760px;">
                            Binary similarity exploration on top of Ghidra BSim. Ingest binaries, compare their
                            functions and whole-binary profiles, cluster what matches, and label the result with tags.
                        </p>
                    </div>
                    <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                        <a class="home-btn home-btn-primary" href="${esc(uploadUrl(lastCollection))}" onclick="Nav.openPath(this.href, event)" style="text-decoration:none;">
                            <i class="fa-solid fa-cloud-arrow-up"></i> Upload binary
                        </a>
                        <button class="home-btn" onclick="Nav.openPath('/collections')">
                            <i class="fa-solid fa-layer-group"></i> Collections
                        </button>
                        <button class="home-btn" onclick="Nav.openPath('/jobs')">
                            <i class="fa-solid fa-server"></i> Jobs
                        </button>
                    </div>
                </div>

                <div onclick="SearchPalette.show()" class="home-card is-clickable" style="cursor:text; display:flex; align-items:center; gap:12px; padding:14px 16px; margin-bottom:24px;">
                    <i class="fa-solid fa-magnifying-glass" style="color:var(--dim);"></i>
                    <span style="color:var(--dim); flex:1;">Search batches, files, functions, clusters, tags, features, collections, pools…</span>
                    <span style="font-size:0.72rem; color:var(--dim); border:1px solid var(--border); border-radius:4px; padding:3px 7px;">Ctrl</span>
                    <span style="font-size:0.72rem; color:var(--dim); border:1px solid var(--border); border-radius:4px; padding:3px 7px;">K</span>
                </div>

                <div id="home-stats" style="margin-bottom:24px;"></div>

                <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(340px, 1fr)); gap:16px; margin-bottom:24px;">
                    <div id="home-jobs"></div>
                    <div id="home-recent"></div>
                </div>

                ${sectionTitle('fa-compass', 'What you can do here')}
                <div id="home-capabilities" style="display:grid; grid-template-columns:repeat(auto-fill, minmax(300px, 1fr)); gap:14px; margin-bottom:28px;"></div>

                <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(340px, 1fr)); gap:16px;">
                    <div id="home-tags"></div>
                    <div id="home-clusters"></div>
                    <div id="home-batches"></div>
                </div>
            </div>
        </div>`;
    }

    function renderCapabilities(lastCollection) {
        document.getElementById('home-capabilities').innerHTML = CAPABILITIES.map(c => `
            <a href="${c.url(lastCollection)}" onclick="Nav.openPath(this.href, event)"
               class="home-card" style="text-decoration:none; color:inherit; display:block; padding:14px 16px;">
                <div style="display:flex; align-items:center; gap:10px; margin-bottom:6px;">
                    <i class="fa-solid ${c.icon}" style="color:var(--dim);"></i>
                    <span style="font-weight:600; font-size:0.9rem;">${c.title}</span>
                </div>
                <div style="font-size:0.78rem; color:var(--dim); line-height:1.45;">${c.blurb}</div>
            </a>`).join('');
    }

    function renderStats(s) {
        const t = s.totals || {};
        const tiles = [
            {
                label: 'Collections', value: t.collections, icon: 'fa-layer-group', url: '/collections',
                tip: 'A collection is one named namespace of binaries. Files, functions, similarities and clusters all live inside a collection.'
            },
            {
                label: 'Pools', value: t.pools, icon: 'fa-diagram-project', url: '/pools',
                tip: 'A pool is a union of collections with its own similarity and cluster namespace — used to compare a corpus against a reference set.'
            },
            {
                label: 'Batches', value: t.batches, icon: 'fa-boxes-stacked', url: '/collections',
                tip: 'One upload is one batch. Batches group the files ingested together so you can track or re-run them as a unit.'
            },
            {
                label: 'Files', value: t.files, icon: 'fa-file-code', url: '/collections',
                tip: 'Every ingested binary, identified by MD5, with its metadata, tags and function count.'
            },
            {
                label: 'Functions', value: t.functions, icon: 'fa-code', url: '/collections',
                tip: 'Decompiled functions across all files. The function is the unit BSim vectorises and compares.'
            }
        ];
        document.getElementById('home-stats').innerHTML =
            `<div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(160px, 1fr)); gap:14px;">` +
            tiles.map(x => `<div class="home-card is-clickable" style="padding:14px 16px;">
                <div style="display:flex; align-items:center; justify-content:space-between; gap:6px; font-size:0.72rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.05em; margin-bottom:6px;">
                    <a href="${x.url}" onclick="Nav.openPath(this.href, event)" style="color:inherit; text-decoration:none;">
                        <i class="fa-solid ${x.icon}"></i> ${x.label}</a>
                    ${tip(x.tip)}
                </div>
                <a href="${x.url}" onclick="Nav.openPath(this.href, event)" style="color:inherit; text-decoration:none; display:block; font-size:1.5rem; font-weight:600;">${num(x.value)}</a>
            </div>`).join('') +
            `</div>`;
    }

    function renderJobs(s) {
        const j = s.jobs || {};
        const jobs = (s.recent_jobs || []).slice(0, 6);
        const chip = (label, val, cls) =>
            `<span class="job-status-badge status-${cls}">${label} ${num(val)}</span>`;

        const rows = jobs.map(job => {
            const url = job.collection ? `/collections/${encodeURIComponent(job.collection)}/jobs` : '/jobs';
            const dur = typeof window.formatDuration === 'function'
                ? formatDuration(Number(job.created_at) || 0, Number(job.updated_at) || 0, job.status) : '—';
            return `<tr>
                <td class="ellip"><a href="${url}" onclick="Nav.openPath(this.href, event)" title="${esc(job.type)}">${esc(job.type)}</a>
                    <div style="color:var(--dim); font-size:0.7rem; overflow:hidden; text-overflow:ellipsis;">${esc(job.collection || job.pool || '—')}</div></td>
                <td>${statusBadge(job.status)}</td>
                <td style="min-width:110px;">${progressBar(job)}</td>
                <td class="num" style="color:var(--dim);">${dur}</td>
            </tr>`;
        });

        document.getElementById('home-jobs').innerHTML = card(
            sectionTitle('fa-server', 'Job queue', `<a href="/jobs" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); text-decoration:none;">All jobs →</a>`) +
            `<div style="display:flex; flex-wrap:wrap; gap:8px; margin-bottom:12px;">
                ${chip('running', j.processing ?? j.running ?? 0, 'running')}
                ${chip('pending', j.pending ?? 0, 'pending')}
                ${chip('completed', j.completed ?? 0, 'completed')}
                ${chip('failed', j.failed ?? 0, 'failed')}
            </div>` +
            table(
                [{ label: 'Job' }, { label: 'Status' }, { label: 'Progress' }, { label: 'Duration', align: 'right' }],
                rows, 'No jobs yet.')
        );
    }

    function renderRecent(s) {
        const rows = (s.recent_collections || []).map(c => {
            const url = `/collections/${encodeURIComponent(c.name)}`;
            return `<tr>
                <td class="ellip"><a href="${url}" onclick="Nav.openPath(this.href, event)" title="${esc(c.name)}">${esc(c.name)}</a></td>
                <td class="num">${num(c.total_files)}</td>
                <td class="num">${num(c.total_functions)}</td>
                <td class="num" style="color:var(--dim);">${ago(c.last_updated)}</td>
            </tr>`;
        });
        document.getElementById('home-recent').innerHTML = card(
            sectionTitle('fa-clock-rotate-left', 'Recently updated collections',
                `<a href="/collections" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); text-decoration:none;">All collections →</a>`) +
            table(
                [{ label: 'Collection' }, { label: 'Files', align: 'right' }, { label: 'Functions', align: 'right' }, { label: 'Updated', align: 'right' }],
                rows, 'Nothing ingested yet — start from Upload binary.')
        );
    }

    function loading(el, label) {
        document.getElementById(el).innerHTML = card(
            `<div style="color:var(--dim); font-size:0.8rem;"><i class="fa-solid fa-spinner fa-spin"></i> ${label}</div>`);
    }

    function renderInsights(d) {
        const tags = (d.tags && d.tags.top) || [];
        const ns = (d.tags && d.tags.namespaces) || [];
        const maxTag = tags.length ? tags[0].count : 1;
        const tagRows = tags.slice(0, 12).map(t => `<tr>
            <td class="ellip" title="${esc(t.tag)}">${esc(t.tag)}</td>
            <td style="width:45%;">
                <div class="job-progress-track"><div class="job-progress-fill" style="width:${Math.max(2, 100 * t.count / maxTag)}%; background:var(--accent);"></div></div>
            </td>
            <td class="num">${num(t.count)}</td></tr>`);
        document.getElementById('home-tags').innerHTML = card(
            sectionTitle('fa-tags', 'Top tags') +
            table([{ label: 'Tag' }, { label: 'Share' }, { label: 'Files', align: 'right' }], tagRows, 'No tags indexed yet.') +
            (ns.length ? `<div style="margin-top:12px; padding-top:10px; border-top:1px solid var(--border); display:flex; flex-wrap:wrap; gap:6px;">` +
                ns.map(n => `<span style="font-size:0.7rem; padding:2px 7px; border-radius:10px; background:var(--hover); color:var(--dim);">${esc(n.namespace)} · ${num(n.count)}</span>`).join('') +
                `</div>` : '')
        );

        const clsRows = (d.biggest_clusters || []).map(c => {
            const url = `/collections/${encodeURIComponent(c.collection)}/files/clusters`;
            return `<tr>
                <td class="ellip"><a href="${url}" onclick="Nav.openPath(this.href, event)">${esc(c.cluster_name || ('cluster ' + c.cluster_id))}</a></td>
                <td class="ellip" style="color:var(--dim);">${esc(c.collection)}</td>
                <td class="num">${num(c.count)}</td></tr>`;
        });
        document.getElementById('home-clusters').innerHTML = card(
            sectionTitle('fa-bullseye', 'Biggest binary clusters') +
            table([{ label: 'Cluster' }, { label: 'Collection' }, { label: 'Files', align: 'right' }], clsRows, 'No binary clusters built yet.')
        );

        const batchRows = (d.recent_batches || []).map(x => {
            const url = `/collections/${encodeURIComponent(x.collection)}/batches`;
            return `<tr>
                <td class="ellip"><a href="${url}" onclick="Nav.openPath(this.href, event)">${esc(x.batch_name || x.batch_uuid)}</a></td>
                <td class="ellip" style="color:var(--dim);">${esc(x.collection)}</td>
                <td class="num" style="color:var(--dim);">${ago(x.last_updated)}</td></tr>`;
        });
        document.getElementById('home-batches').innerHTML = card(
            sectionTitle('fa-boxes-stacked', 'Recent batches') +
            table([{ label: 'Batch' }, { label: 'Collection' }, { label: 'Updated', align: 'right' }], batchRows, 'No batches yet.')
        );
    }

    async function init(params, containerId) {
        container = document.getElementById(containerId || 'module-view-container');
        if (!container) return;
        container.innerHTML = shell(localStorage.getItem('lastCollectionContext'));

        renderCapabilities(localStorage.getItem('lastCollectionContext'));

        try {
            const s = await (await fetch('/api/index/home/stats')).json();
            renderStats(s);
            renderJobs(s);
            renderRecent(s);
        } catch (e) {
            document.getElementById('home-stats').innerHTML = card(
                `<div style="color:#f92672; font-size:0.8rem;">Could not load instance stats: ${esc(e.message)}</div>`);
        }

        // Heavier panels, server-cached: never block the page above.
        ['home-tags', 'home-clusters', 'home-batches'].forEach(id => loading(id, 'Computing…'));
        try {
            renderInsights(await (await fetch('/api/index/home/insights')).json());
        } catch (e) {
            ['home-tags', 'home-clusters', 'home-batches'].forEach(id => {
                document.getElementById(id).innerHTML = card(
                    `<div style="color:#f92672; font-size:0.8rem;">Insights failed: ${esc(e.message)}</div>`);
            });
        }
    }

    function destroy() { container = null; }

    return { init, destroy };
})();
