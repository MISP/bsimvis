/**
 * Pool Detail View
 * Loaded when navigating to /pools/{id}
 */

window.PoolDetailView = {
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

        const poolId = params.pool || params.pool_id;
        if (!poolId) {
            container.innerHTML = '<div style="padding:30px; color:#f87171;">Error: No pool ID provided.</div>';
            return;
        }

        container.innerHTML = `
            <div style="display:flex; justify-content:center; align-items:center; height:200px; color:var(--dim); font-size:1rem;">
                <i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading Pool Details...
            </div>`;

        try {
            const [poolRes, collectionsRes, jobsStatsRes] = await Promise.all([
                fetch(`/api/pool/${encodeURIComponent(poolId)}`),
                fetch('/api/collection/search?limit=500&offset=0'),
                fetch('/api/jobs/stats').catch(() => null)
            ]);

            if (!poolRes.ok) throw new Error('Pool not found');
            const pool = await poolRes.json();

            // Pre-warm pool name in breadcrumb cache
            if (typeof Breadcrumbs !== 'undefined' && Breadcrumbs.setPoolName) {
                Breadcrumbs.setPoolName(poolId, pool.name || poolId);
                Breadcrumbs.refresh();
            }

            let allCollections = [];
            if (collectionsRes.ok) {
                const cd = await collectionsRes.json();
                allCollections = cd.collections || [];
            }

            // Map collection name → stats
            const collMap = {};
            allCollections.forEach(c => { collMap[c.name] = c; });

            let poolActiveJobs = [];
            if (jobsStatsRes && jobsStatsRes.ok) {
                const stats = await jobsStatsRes.json();
                const activeJobs = stats.active_jobs || [];
                poolActiveJobs = activeJobs.filter(job => {
                    if (job.pool_id === poolId) return true;
                    if (job.collection === `pool:${poolId}`) return true;
                    if (pool.collections && pool.collections.includes(job.collection)) return true;
                    return false;
                });
            }

            container.innerHTML = this._renderPage(pool, poolId, collMap, poolActiveJobs);

            if (this.refreshInterval) clearInterval(this.refreshInterval);
            this.refreshInterval = setInterval(async () => {
                try {
                    const res = await fetch('/api/jobs/stats');
                    if (res.ok) {
                        const stats = await res.json();
                        const activeJobs = stats.active_jobs || [];
                        const filtered = activeJobs.filter(job => {
                            if (job.pool_id === poolId) return true;
                            if (job.collection === `pool:${poolId}`) return true;
                            if (pool.collections && pool.collections.includes(job.collection)) return true;
                            return false;
                        });
                        const tableContainer = document.getElementById('active-jobs-container');
                        if (tableContainer) {
                            tableContainer.innerHTML = this._renderActiveJobsSection(poolId, filtered);
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

    _statusBadge(status) {
        if (status === 'current') return `<span style="background:rgba(16,185,129,0.15); border:1px solid rgba(16,185,129,0.35); color:#10b981; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle-check"></i> Current</span>`;
        if (status === 'outdated') return `<span style="background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.35); color:#f59e0b; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle-exclamation"></i> Outdated</span>`;
        return `<span style="background:rgba(156,163,175,0.15); border:1px solid rgba(156,163,175,0.3); color:#9ca3af; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle"></i> ${status || 'created'}</span>`;
    },

    _configRow(label, value) {
        if (value === undefined || value === null || value === '') return '';
        return `<div style="display:flex; justify-content:space-between; align-items:center; padding:7px 0; border-bottom:1px solid rgba(255,255,255,0.04); font-size:0.8rem;">
            <span style="color:var(--dim);">${label}</span>
            <span style="color:var(--accent); font-weight:600; font-family:monospace;">${value}</span>
        </div>`;
    },

    _renderPage(pool, poolId, collMap, poolActiveJobs = []) {
        const status = pool.sync_status || 'created';
        const name = pool.name || 'Unnamed Pool';
        const createdAt = pool.created_at ? (typeof window.formatDate === 'function' ? formatDate(pool.created_at) : pool.created_at) : '—';
        const lastBuilt = pool.last_built_at && pool.last_built_at != '0' ? (typeof window.formatDate === 'function' ? formatDate(parseInt(pool.last_built_at)) : pool.last_built_at) : '—';

        const fs = pool.func_sim_params || {};
        const fc = pool.func_cluster_params || {};
        const fis = pool.file_sim_params || {};
        const fic = pool.file_cluster_params || {};
        const crossOnly = pool.only_cross_collection;

        const buildBtnHtml = status === 'outdated'
            ? `<button onclick="window.poolDetailBuild('${poolId}', this)" style="background:rgba(59,130,246,0.12); border:1px solid rgba(59,130,246,0.35); color:#60a5fa; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px; box-sizing:border-box; white-space:nowrap; flex-shrink:0;"><i class="fa-solid fa-play"></i> Build</button>`
            : '';

        let totalPoolBatches = 0;
        let totalPoolFiles = 0;
        let totalPoolFunctions = 0;

        (pool.collections || []).forEach(cname => {
            const stats = collMap[cname] || {};
            if (typeof stats.total_batches === 'number') totalPoolBatches += stats.total_batches;
            if (typeof stats.total_files === 'number') totalPoolFiles += stats.total_files;
            if (typeof stats.total_functions === 'number') totalPoolFunctions += stats.total_functions;
        });

        const collectionsHtml = `
        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
            <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border); background:rgba(255,255,255,0.02); color:var(--dim);">
                        <th style="padding:10px 15px;">Collection</th>
                        <th style="padding:10px 15px; text-align:right;">Batches</th>
                        <th style="padding:10px 15px; text-align:right;">Files</th>
                        <th style="padding:10px 15px; text-align:right;">Functions</th>
                    </tr>
                </thead>
                <tbody>
                    ${(pool.collections || []).map(cname => {
                        const stats = collMap[cname] || {};
                        const files = stats.total_files !== undefined ? stats.total_files : 0;
                        const funcs = stats.total_functions !== undefined ? stats.total_functions : 0;
                        const batches = stats.total_batches !== undefined ? stats.total_batches : 0;
                        const collUrl = `/collections/${encodeURIComponent(cname)}`;
                        const filesUrl = `/collections/${encodeURIComponent(cname)}/files`;
                        const funcsUrl = `/collections/${encodeURIComponent(cname)}/functions`;
                        const batchesUrl = `/collections/${encodeURIComponent(cname)}/batches`;
                        return `
                        <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                            <td style="padding:10px 15px;"><a href="${collUrl}" onclick="Nav.openPath('${collUrl}', event)" style="font-weight:bold; color:var(--text); text-decoration:none;">${cname}</a></td>
                            <td style="padding:10px 15px; text-align:right;"><a href="${batchesUrl}" onclick="Nav.openPath('${batchesUrl}', event)" class="clickable-count" style="color:var(--accent); font-weight:700; text-decoration:none;">${batches}</a></td>
                            <td style="padding:10px 15px; text-align:right;"><a href="${filesUrl}" onclick="Nav.openPath('${filesUrl}', event)" class="clickable-count" style="color:#60a5fa; font-weight:700; text-decoration:none;">${files}</a></td>
                            <td style="padding:10px 15px; text-align:right;"><a href="${funcsUrl}" onclick="Nav.openPath('${funcsUrl}', event)" class="clickable-count" style="color:#a78bfa; font-weight:700; text-decoration:none;">${funcs}</a></td>
                        </tr>`;
                    }).join('')}
                </tbody>
            </table>
        </div>`;

        const funcSims = pool.total_func_similarities !== undefined ? pool.total_func_similarities : 0;
        const funcClusts = pool.total_func_clusters !== undefined ? pool.total_func_clusters : 0;
        const fileSims = pool.total_file_similarities !== undefined ? pool.total_file_similarities : 0;
        const fileClusts = pool.total_file_clusters !== undefined ? pool.total_file_clusters : 0;
        const fileSimEnabled = fis.enabled !== false;
        const collectionCount = (pool.collections || []).length;

        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:22px;">

            <!-- HEADER -->
            <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap;">
                <div>
                    <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px; flex-wrap:wrap;">
                        <i class="fa-solid fa-diagram-project" style="font-size:1.4rem; color:var(--accent);"></i>
                        <h1 style="margin:0; font-size:1.5rem; color:var(--text);">${name}</h1>
                        ${crossOnly ? `<span style="background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.3); color:#f59e0b; padding:3px 10px; border-radius:20px; font-size:0.72rem; font-weight:700; display:inline-flex; align-items:center; gap:5px;"><i class="fa-solid fa-arrow-right-arrow-left"></i> Cross-Only</span>` : ''}
                    </div>
                    <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                        <code style="font-size:0.8rem; color:var(--dim); background:rgba(255,255,255,0.04); padding:3px 10px; border-radius:4px; border:1px solid var(--border);">${poolId}</code>
                        <button onclick="copyToClipboard('${poolId}', this)" class="btn-copy" title="Copy Pool ID" style="padding:4px 8px; font-size:0.75rem;">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </div>
                <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                    <button onclick="Nav.openPath('/pools/${encodeURIComponent(poolId)}/jobs')" style="background:rgba(59,130,246,0.12); border:1px solid rgba(59,130,246,0.35); color:#60a5fa; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px; box-sizing:border-box; white-space:nowrap; flex-shrink:0;"><i class="fa-solid fa-server"></i> View Jobs</button>
                    ${buildBtnHtml}
                    <button onclick="window.poolDetailRebuild('${poolId}', this)" style="background:rgba(168,85,247,0.12); border:1px solid rgba(168,85,247,0.35); color:#c084fc; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px; box-sizing:border-box; white-space:nowrap; flex-shrink:0;"><i class="fa-solid fa-rotate"></i> Rebuild</button>
                    <button onclick="window.poolDetailDelete('${poolId}', this)" style="background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.3); color:#f87171; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px; box-sizing:border-box; white-space:nowrap; flex-shrink:0;"><i class="fa-solid fa-trash-can"></i> Delete</button>
                </div>
            </div>

            <!-- META ROW -->
            <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:center;">
                <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px 18px; font-size:0.8rem; display:flex; align-items:center; gap:8px;">
                    <i class="fa-solid fa-calendar-plus" style="color:var(--dim);"></i>
                    <span style="color:var(--dim);">Created</span>
                    <span style="color:var(--text); font-weight:600;">${createdAt}</span>
                </div>
                <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px 18px; font-size:0.8rem; display:flex; align-items:center; gap:8px;">
                    <i class="fa-solid fa-clock-rotate-left" style="color:var(--dim);"></i>
                    <span style="color:var(--dim);">Last Built</span>
                    <span style="color:var(--text); font-weight:600;">${lastBuilt}</span>
                </div>
                ${this._statusBadge(status)}
            </div>

            <!-- MAIN GRID: left = pool metadata + collections, right = similarity params -->
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:20px; align-items:start;">

                <!-- LEFT COLUMN -->
                <div style="display:flex; flex-direction:column; gap:20px;">

                    <!-- Pool Metadata -->
                    <div>
                        <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); margin-bottom:12px; display:flex; align-items:center; gap:7px;">
                            <i class="fa-solid fa-circle-info"></i> Pool Metadata
                        </div>
                        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                            <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                                <thead>
                                    <tr style="border-bottom:1px solid var(--border); background:rgba(255,255,255,0.02); color:var(--dim);">
                                        <th style="padding:10px 15px;">Entity Type</th>
                                        <th style="padding:10px 15px; text-align:right;">Count / Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-layer-group" style="margin-right:8px; width:16px;"></i>Collections</td>
                                        <td style="padding:10px 15px; text-align:right; color:var(--accent); font-weight:700;">${collectionCount}</td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-file-code" style="margin-right:8px; width:16px;"></i>Files</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/pools/${encodeURIComponent(poolId)}/files" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${totalPoolFiles}</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-code" style="margin-right:8px; width:16px;"></i>Functions</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/pools/${encodeURIComponent(poolId)}/functions" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${totalPoolFunctions}</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-code-compare" style="margin-right:8px; width:16px;"></i>Function Similarities</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/pools/${encodeURIComponent(poolId)}/functions/similarities" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${funcSims}</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-bullseye" style="margin-right:8px; width:16px;"></i>Function Clusters</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/pools/${encodeURIComponent(poolId)}/functions/clusters" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${funcClusts}</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-right-left" style="margin-right:8px; width:16px;"></i>File Similarities</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/pools/${encodeURIComponent(poolId)}/files/similarities" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${fileSims}</a></td>
                                    </tr>
                                    <tr style="transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-bullseye" style="margin-right:8px; width:16px;"></i>File Clusters</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/pools/${encodeURIComponent(poolId)}/files/clusters" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${fileClusts}</a></td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>

                    <!-- Collections -->
                    <div>
                        <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); margin-bottom:12px; display:flex; align-items:center; gap:7px;">
                            <i class="fa-solid fa-layer-group"></i> Collections
                        </div>
                        <div style="display:flex; flex-direction:column; gap:10px;">
                            ${collectionsHtml || '<div style="color:var(--dim); font-size:0.85rem; padding:20px; text-align:center; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">No collections</div>'}
                        </div>
                    </div>
                </div>

                <!-- RIGHT COLUMN: similarity params -->
                <div style="display:flex; flex-direction:column; gap:16px;">
                    <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); display:flex; align-items:center; gap:7px;">
                        <i class="fa-solid fa-sliders"></i> Similarity Parameters
                    </div>

                    <!-- Func Sim -->
                    <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:10px; padding:18px;">
                        <div style="font-size:0.72rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--accent); margin-bottom:12px; display:flex; align-items:center; gap:7px; border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:10px;">
                            <i class="fa-solid fa-microchip"></i> Function Similarity
                        </div>
                        ${this._configRow('Algorithm', fs.algo)}
                        ${this._configRow('Top K', fs.top_k)}
                        ${this._configRow('Min Score', fs.min_score)}
                        ${this._configRow('Min Cluster Size', fc.min_cluster_size)}
                        ${this._configRow('Min Samples', fc.min_samples)}
                        ${this._configRow('Epsilon', fc.epsilon)}
                        ${this._configRow('Method', fc.selection_method)}
                    </div>

                    <!-- File Sim -->
                    <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:10px; padding:18px; ${fileSimEnabled ? '' : 'opacity:0.5;'}">
                        <div style="font-size:0.72rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:${fileSimEnabled ? '#60a5fa' : 'var(--dim)'}; margin-bottom:12px; display:flex; align-items:center; gap:7px; border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:10px;">
                            <i class="fa-solid fa-file-code"></i> File Similarity ${fileSimEnabled ? '' : '<span style="font-size:0.65rem; margin-left:4px; color:var(--dim);">(disabled)</span>'}
                        </div>
                        ${this._configRow('Algorithm', fis.algo)}
                        ${this._configRow('Top K', fis.top_k)}
                        ${this._configRow('Min Score', fis.min_score)}
                        ${this._configRow('Min Cluster Size', fic.min_cluster_size)}
                        ${this._configRow('Min Samples', fic.min_samples)}
                        ${this._configRow('Epsilon', fic.epsilon)}
                        ${this._configRow('Method', fic.selection_method)}
                    </div>
                </div>
            </div>

            <!-- ACTIVE / RUNNING JOBS -->
            <div id="active-jobs-container">
                ${this._renderActiveJobsSection(poolId, poolActiveJobs)}
            </div>

        </div>`;
    },

    _renderActiveJobsSection(poolId, poolActiveJobs) {
        return `
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
                    <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); display:flex; align-items:center; gap:7px;">
                        <i class="fa-solid fa-server"></i> Active Jobs
                    </div>
                    <button onclick="Nav.openPath('/pools/${encodeURIComponent(poolId)}/jobs')" style="background:rgba(255,255,255,0.03); border:1px solid var(--border); color:var(--text); padding:5px 12px; border-radius:4px; font-size:0.75rem; font-weight:600; cursor:pointer; display:inline-flex; align-items:center; gap:6px; transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.07)'" onmouseout="this.style.background='rgba(255,255,255,0.03)'"><i class="fa-solid fa-server"></i> See All Jobs</button>
                </div>
                ${poolActiveJobs.length > 0 ? `
                <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                    <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                        <thead>
                            <tr style="border-bottom:1px solid var(--border); background:rgba(255,255,255,0.02); color:var(--dim);">
                                <th style="padding:10px 15px;">Job ID</th>
                                <th style="padding:10px 15px;">Type</th>
                                <th style="padding:10px 15px;">Status</th>
                                <th style="padding:10px 15px;">Progress</th>
                                <th style="padding:10px 15px; text-align:right;">Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${poolActiveJobs.map(job => {
                                const jobUrl = `/pools/${encodeURIComponent(poolId)}/jobs`;
                                const status = job.status;
                                let actions = '<div class="job-actions" style="display:flex; justify-content:flex-end;">';
                                if (status === 'pending' || status === 'running') {
                                    actions += `<button class="job-btn-action danger" onclick="cancelJob('${job.id}')" title="Cancel Job"><i class="fa-solid fa-ban"></i></button>`;
                                }
                                if (status === 'failed' || status === 'cancelled' || status === 'completed') {
                                    actions += `<button class="job-btn-action" onclick="retryJob('${job.id}')" title="Retry/Resume Job"><i class="fa-solid fa-rotate-right"></i></button>`;
                                }
                                actions += `<button class="job-btn-action info" onclick="showJobDetails('${job.id}')" title="View Logs & Details"><i class="fa-solid fa-circle-info"></i></button>`;
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

                                return `
                                <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                    <td style="padding:10px 15px; font-family:monospace;"><a href="${jobUrl}" onclick="Nav.openPath('${jobUrl}', event)" style="color:var(--accent); text-decoration:none; font-weight:600;">${job.id}</a></td>
                                    <td style="padding:10px 15px; font-weight:600; text-transform:capitalize;">${job.type}</td>
                                    <td style="padding:10px 15px;">${statusBadge}</td>
                                    <td style="padding:10px 15px;">${progressHtml}</td>
                                    <td style="padding:10px 15px; text-align:right;">${actions}</td>
                                </tr>`;
                            }).join('')}
                        </tbody>
                    </table>
                </div>` : `
                <div style="color:var(--dim); font-size:0.85rem; padding:20px; text-align:center; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">
                    No active jobs running for this pool.
                </div>
                `}`;
    }
};

// Action handlers scoped to this view
window.poolDetailBuild = async function(poolId, btn) {
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch(`/api/pool/${encodeURIComponent(poolId)}/build`, { method: 'POST' });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        alert(`Pool build enqueued! Job ID: ${data.job_id}`);
        if (typeof refreshData === 'function') refreshData(false, true);
        else Nav.openPath(window.location.pathname);
    } catch(e) {
        alert(`Failed to build pool: ${e.message}`);
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-play"></i> Build'; }
    }
};

window.poolDetailRebuild = async function(poolId, btn) {
    if (!confirm(`Wipe and rebuild all data for pool "${poolId}"?`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch(`/api/pool/${encodeURIComponent(poolId)}/rebuild`, { method: 'POST' });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        alert(`Pool rebuild enqueued! Job ID: ${data.job_id}`);
        if (typeof refreshData === 'function') refreshData(false, true);
        else Nav.openPath(window.location.pathname);
    } catch(e) {
        alert(`Failed to rebuild pool: ${e.message}`);
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-rotate"></i> Rebuild'; }
    }
};

window.poolDetailDelete = async function(poolId, btn) {
    if (!confirm(`Delete pool "${poolId}"? This cannot be undone.`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch(`/api/pool/${encodeURIComponent(poolId)}`, { method: 'DELETE' });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        Nav.openPath('/pools');
    } catch(e) {
        alert(`Failed to delete pool: ${e.message}`);
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-trash-can"></i> Delete'; }
    }
};
