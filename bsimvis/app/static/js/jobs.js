/**
 * Jobs UI Module for BSimVis
 */

const collapsedPipelines = new Set(JSON.parse(localStorage.getItem('collapsedPipelines') || '[]'));
const loadedSubtasks = new Map();
// ponytail: tracks pipelines user explicitly expanded so auto-collapse doesn't re-close them
const userExpandedPipelines = new Set(JSON.parse(localStorage.getItem('userExpandedPipelines') || '[]'));

function parseCollectionContext(col) {
    if (!col) return { pool: null, collection: 'main' };
    const mPool = col.match(/^(?:global:)?pool:([^:]+)(?::col:(.+))?$/);
    if (mPool) {
        return {
            pool: mPool[1],
            collection: mPool[2] || 'main'
        };
    }
    return { pool: null, collection: col };
}

function getJobTargetLink(job) {
    let rawTarget = '';
    let isMd5 = false;
    
    if (job.payload) {
        try {
            const payload = typeof job.payload === 'string' ? JSON.parse(job.payload) : job.payload;
            rawTarget = payload.md5 || payload.file_id || payload.batch_uuid || '';
            isMd5 = !!payload.md5;
        } catch(e) {}
    }
    
    if (!rawTarget && job.target) {
        rawTarget = job.target;
        if (rawTarget.length === 32 || (rawTarget.includes('...') && rawTarget.length === 19)) {
            isMd5 = true;
        }
    }
    
    if (!rawTarget) return '<span class="dim">-</span>';
    
    let collectionStr = job.collection || '';
    if (!collectionStr && job.payload) {
        try {
            const payload = typeof job.payload === 'string' ? JSON.parse(job.payload) : job.payload;
            collectionStr = payload.collection || (payload.pool_id ? `pool:${payload.pool_id}` : '');
        } catch(e) {}
    }
    
    const displayTarget = rawTarget.length > 20 ? rawTarget.slice(0, 8) + '...' + rawTarget.slice(-8) : rawTarget;
    
    if (isMd5) {
        const ctx = parseCollectionContext(collectionStr);
        const activePool = ctx.pool || (window.getRoutingState ? window.getRoutingState().pool : null);
        
        if (activePool) {
            const prefix = window.location.pathname.startsWith('/pool/') ? 'pool' : 'pools';
            const url = `/${prefix}/${encodeURIComponent(activePool)}/collections/${encodeURIComponent(ctx.collection)}/files/${encodeURIComponent(rawTarget)}`;
            return `<a onclick="window.Nav && window.Nav.openPath(${escapeAttr(jsString(url))});" class="job-target-link" title="View File in Pool ${escapeAttr(activePool)}"><i class="fa-solid fa-file-code"></i> <code class="job-target-text">${escapeHtml(displayTarget)}</code></a>`;
        } else if (ctx.collection) {
            const url = `/collections/${encodeURIComponent(ctx.collection)}/files/${encodeURIComponent(rawTarget)}`;
            return `<a onclick="window.Nav && window.Nav.openPath(${escapeAttr(jsString(url))});" class="job-target-link" title="View File Details"><i class="fa-solid fa-file-code"></i> <code class="job-target-text">${escapeHtml(displayTarget)}</code></a>`;
        }
    }
    
    return `<code class="job-target-text" title="${escapeAttr(rawTarget)}">${escapeHtml(displayTarget)}</code>`;
}

// Job Type to FontAwesome Icon mapping
const JOB_TYPE_ICONS = {
    'pipeline': 'fa-microchip',
    'group': 'fa-layer-group',
    'file_data_ingest': 'fa-file-import',
    'ghidra_analyze': 'fa-gears',
    'idx_meta': 'fa-tags',
    'idx_functions': 'fa-code',
    'idx_features': 'fa-brain',
    'build_sim': 'fa-diagram-project',
    'index_sim': 'fa-magnifying-glass',
    'clear_sim': 'fa-trash-can',
    'clear_features': 'fa-trash-can',
    'sync_milvus': 'fa-arrows-rotate',
    'cluster_functions': 'fa-cubes',
    'clear_cluster': 'fa-trash-can',
    'cluster_binaries': 'fa-cubes',
    'clear_bin_cluster': 'fa-trash-can',
    'build_bin_sim': 'fa-diagram-project',
    'clear_bin_sim': 'fa-trash-can',
    'reindex_bin_sim': 'fa-arrows-rotate',
    'enrich_features': 'fa-wand-magic',
    'delete_collection': 'fa-trash-can',
    'clean_collection': 'fa-broom',
    'propagate_metadata': 'fa-share-nodes',
    'build_pool_sim': 'fa-sitemap',
    'cluster_pool': 'fa-cubes',
    'init_pool_build': 'fa-play',
    'finalize_pool_build': 'fa-circle-check',
    'build_pool_bin_sim': 'fa-sitemap',
    'cluster_pool_binaries': 'fa-cubes'
};

let lastJobsData = null;
const activeSubtaskFetches = new Set();

async function refreshPipelineSubtasks(pipelineId) {
    if (activeSubtaskFetches.has(pipelineId)) return;
    activeSubtaskFetches.add(pipelineId);
    try {
        const resp = await fetch(`/api/jobs/${pipelineId}`);
        if (resp.ok) {
            const data = await resp.json();
            if (data.sub_tasks) {
                loadedSubtasks.set(pipelineId, data.sub_tasks);
                // Re-render table using lastJobsData if view is still jobs
                const { viewKey } = window.getRoutingState ? window.getRoutingState() : { viewKey: '' };
                if (viewKey === 'jobs' && lastJobsData) {
                    const tbody = document.getElementById('table-body');
                    if (tbody) {
                        tbody.innerHTML = renderJobs(lastJobsData, true);
                    }
                }
            }
        }
    } catch (e) {
        console.error("Failed to update subtasks for", pipelineId, e);
    } finally {
        activeSubtaskFetches.delete(pipelineId);
    }
}

window.togglePipelineCollapse = async function (pipelineId) {
    if (collapsedPipelines.has(pipelineId)) {
        collapsedPipelines.delete(pipelineId);
        userExpandedPipelines.add(pipelineId);
        localStorage.setItem('userExpandedPipelines', JSON.stringify(Array.from(userExpandedPipelines)));

        // Fetch subtask data if expanding and not loaded yet
        if (!loadedSubtasks.has(pipelineId)) {
            try {
                const resp = await fetch(`/api/jobs/${pipelineId}`);
                if (resp.ok) {
                    const data = await resp.json();
                    if (data.sub_tasks) {
                        loadedSubtasks.set(pipelineId, data.sub_tasks);
                    }
                }
            } catch (e) {
                console.error("Failed to load subtasks for pipeline", pipelineId, e);
            }
        }
    } else {
        collapsedPipelines.add(pipelineId);
        userExpandedPipelines.delete(pipelineId);
        localStorage.setItem('userExpandedPipelines', JSON.stringify(Array.from(userExpandedPipelines)));
    }
    localStorage.setItem('collapsedPipelines', JSON.stringify(Array.from(collapsedPipelines)));
    if (window.refreshData) window.refreshData(false, false);
};

window.collapseAllPipelines = function (jobs) {
    const list = lastJobsData || (jobs ? (Array.isArray(jobs) ? jobs : (jobs.results || [])) : []);
    list.forEach(j => {
        if (j.type === 'pipeline' || j.type === 'group') collapsedPipelines.add(j.id);
    });
    userExpandedPipelines.clear();
    localStorage.setItem('userExpandedPipelines', '[]');
    localStorage.setItem('collapsedPipelines', JSON.stringify(Array.from(collapsedPipelines)));
    if (window.refreshData) window.refreshData(false, false);
};

window.expandAllPipelines = function () {
    collapsedPipelines.clear();
    userExpandedPipelines.clear();
    localStorage.setItem('collapsedPipelines', '[]');
    localStorage.setItem('userExpandedPipelines', '[]');
    if (window.refreshData) window.refreshData(false, false);
};

function renderJobs(jobs, skipBackgroundFetch = false) {
    let jobsList = Array.isArray(jobs) ? jobs : (jobs.results || []);
    lastJobsData = jobsList;

    if (jobsList.length === 0) {
        return '<tr><td colspan="8" style="text-align:center; padding: 60px; color: var(--dim);"><i class="fa-solid fa-wind" style="font-size: 2rem; opacity: 0.2; display: block; margin-bottom: 10px;"></i>No recent jobs found</td></tr>';
    }

    // Evict stale loadedSubtasks entries whose parent is no longer reachable from current list roots
    const currentIds = new Set(jobsList.map(j => j.id));
    const activeParentIds = new Set();
    function markReachable(id) {
        if (activeParentIds.has(id)) return;
        activeParentIds.add(id);
        const subTasks = loadedSubtasks.get(id) || [];
        subTasks.forEach(st => {
            if (st.type === 'pipeline' || st.type === 'group') {
                markReachable(st.id);
            }
        });
    }
    currentIds.forEach(id => markReachable(id));

    loadedSubtasks.forEach((_, parentId) => {
        if (!activeParentIds.has(parentId)) loadedSubtasks.delete(parentId);
    });

    // Populate all known jobs into a map
    const jobsById = new Map();
    jobsList.forEach(job => {
        jobsById.set(job.id, job);
    });

    // Merge loaded subtasks
    loadedSubtasks.forEach((subTasks, parentId) => {
        subTasks.forEach(st => {
            if (!jobsById.has(st.id)) {
                const stWithParent = { ...st, parent_id: parentId };
                jobsById.set(st.id, stWithParent);
            }
        });
    });

    // Automatically collapse completed/non-active pipelines that the user hasn't explicitly interacted with
    jobsById.forEach(job => {
        const isPipeline = job.type === 'pipeline' || job.type === 'group';
        if (isPipeline) {
            const isActive = job.status === 'running' || job.status === 'pending';
            const userInteracted = collapsedPipelines.has(job.id) || userExpandedPipelines.has(job.id);
            if (!isActive && !userInteracted) {
                collapsedPipelines.add(job.id);
            }
        }
    });

    const pipelineChildren = new Map();
    const childJobIds = new Set();

    // Map parent-child relationships
    jobsById.forEach(job => {
        if (job.parent_id) {
            childJobIds.add(job.id);
            if (!pipelineChildren.has(job.parent_id)) {
                pipelineChildren.set(job.parent_id, []);
            }
            pipelineChildren.get(job.parent_id).push(job);
        }
        if ((job.type === 'pipeline' || job.type === 'group') && job.task_ids) {
            job.task_ids.forEach(tid => {
                childJobIds.add(tid);
            });
        }
    });

    // Order pipeline/group children by their task_ids array sequence
    jobsById.forEach(job => {
        if (job.type === 'pipeline' || job.type === 'group') {
            const children = [];
            const taskIds = job.task_ids || [];
            taskIds.forEach(tid => {
                const childJob = jobsById.get(tid);
                if (childJob) {
                    children.push(childJob);
                }
            });
            // Append any other children not in task_ids
            const otherChildren = pipelineChildren.get(job.id) || [];
            otherChildren.forEach(child => {
                if (!children.some(c => c.id === child.id)) {
                    children.push(child);
                }
            });
            pipelineChildren.set(job.id, children);
        }
    });

    // Identify true roots
    const topLevelItems = Array.from(jobsById.values()).filter(job => {
        if (job.parent_id && jobsById.has(job.parent_id)) {
            return false;
        }
        if (childJobIds.has(job.id)) {
            let hasParentInList = false;
            for (let pJob of jobsById.values()) {
                if ((pJob.type === 'pipeline' || pJob.type === 'group') && pJob.task_ids && pJob.task_ids.includes(job.id)) {
                    hasParentInList = true;
                    break;
                }
            }
            if (hasParentInList) return false;
        }
        return true;
    });

    // Sort roots by created_at desc
    topLevelItems.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

    // Queue up background updates for expanded pipelines/groups
    if (!skipBackgroundFetch) {
        jobsById.forEach(job => {
            const isPipeline = job.type === 'pipeline' || job.type === 'group';
            const isCollapsed = collapsedPipelines.has(job.id);
            const isLoaded = loadedSubtasks.has(job.id);
            const isActive = job.status === 'running' || job.status === 'pending';
            if (isPipeline && !isCollapsed && (!isLoaded || isActive)) {
                refreshPipelineSubtasks(job.id);
            }
        });
    }

    const rows = [];

    function renderJobRow(job, prefix = '', isLast = true, depth = 0, shouldHide = false) {
        const isPipeline = job.type === 'pipeline' || job.type === 'group';
        const progress = job.progress || 0;
        const status = job.status || 'pending';

        let progressClass = '';
        if (status === 'running') progressClass = 'progress-running';
        if (status === 'completed') progressClass = 'progress-completed';
        if (status === 'failed' || status === 'cancelled') progressClass = 'progress-failed';

        const progressHtml = `
            <div class="job-progress-container">
                <div class="job-progress-track">
                    <div class="job-progress-fill ${progressClass}" style="width: ${progress}%"></div>
                </div>
                <span class="job-progress-text">${progress}%</span>
            </div>
        `;

        let statusIcon = 'fa-circle-notch fa-spin';
        if (status === 'completed') statusIcon = 'fa-check-circle';
        if (status === 'failed') statusIcon = 'fa-exclamation-circle';
        if (status === 'cancelled') statusIcon = 'fa-ban';
        if (status === 'pending') statusIcon = 'fa-clock';

        const statusBadge = `<span class="job-status-badge status-${status}"><i class="fa-solid ${statusIcon}"></i> ${status.toUpperCase()}</span>`
            + (job.paused ? ' <span class="job-status-badge" title="Held back; other jobs keep running"><i class="fa-solid fa-pause"></i> PAUSED</span>' : '');
        const createdDate = new Date(job.created_at).toLocaleString();

        let actions = '<div class="job-actions">';
        if (status === 'pending' || status === 'running') {
            actions += job.paused
                ? `<button class="job-btn-action" onclick="resumeJob(${escapeAttr(jsString(job.id))})" title="Resume Job"><i class="fa-solid fa-play"></i></button>`
                : `<button class="job-btn-action" onclick="pauseJob(${escapeAttr(jsString(job.id))})" title="Pause Job (other jobs keep running)"><i class="fa-solid fa-pause"></i></button>`;
            actions += `<button class="job-btn-action danger" onclick="cancelJob(${escapeAttr(jsString(job.id))})" title="Cancel Job"><i class="fa-solid fa-ban"></i></button>`;
        }
        if (status === 'failed' || status === 'cancelled' || status === 'completed') {
            actions += `<button class="job-btn-action" onclick="retryJob(${escapeAttr(jsString(job.id))})" title="Retry/Resume Job"><i class="fa-solid fa-rotate-right"></i></button>`;
        }
        actions += `<button class="job-btn-action info" onclick="showJobDetails(${escapeAttr(jsString(job.id))})" title="View Logs & Details"><i class="fa-solid fa-circle-info"></i></button>`;
        actions += '</div>';

        const isCollapsed = isPipeline && collapsedPipelines.has(job.id);
        const childCount = isPipeline ? (pipelineChildren.get(job.id) || []).length : 0;

        // Tree guide lines
        let treeGuide = '';
        if (depth > 0) {
            const connector = isLast ? '└─ ' : '├─ ';
            treeGuide = `<span class="job-tree-guide">${prefix}${connector}</span>`;
        }

        // Chevron collapse icon
        let chevronHtml = '';
        if (isPipeline) {
            const chevron = isCollapsed ? 'fa-chevron-right' : 'fa-chevron-down';
            chevronHtml = `<i class="fa-solid ${chevron} collapse-chevron" onclick="togglePipelineCollapse(${escapeAttr(jsString(job.id))})" style="cursor: pointer; color: var(--accent); width: 14px; text-align: center; margin-right: 6px;"></i>`;
        } else {
            // Leaf nodes get a spacer so they align with pipeline labels
            chevronHtml = `<span style="width: 20px; display: inline-block;"></span>`;
        }

        // Type icon & text
        const iconClass = JOB_TYPE_ICONS[job.type] || 'fa-square';
        let labelText = job.type;
        if (job.type === 'pipeline') labelText = 'PIPELINE';
        else if (job.type === 'group') labelText = 'GROUP';
        else labelText = labelText.replace(/_/g, ' ').toUpperCase();

        const typeHtml = isPipeline 
            ? `<span class="pipeline-label"><i class="fa-solid ${iconClass}"></i> ${labelText}</span>`
            : `<span class="job-label"><i class="fa-solid ${iconClass}"></i> ${labelText}</span>`;

        const hiddenBadge = isCollapsed && childCount > 0
            ? `<span class="pipeline-hidden-badge">${childCount} hidden</span>`
            : '';

        const firstCellHtml = `
            <div class="job-tree-cell">
                ${treeGuide}
                ${chevronHtml}
                ${typeHtml}
                <code class="job-id-text" title="${job.id}">${job.id}</code>
                ${hiddenBadge}
            </div>
        `;

        const depthClass = depth > 0 ? `child-depth-${Math.min(depth, 3)}` : '';

        let collectionDisplay;
        if (job.collection) {
            const ctx = parseCollectionContext(job.collection);
            if (ctx.pool) {
                const displayName = job.pool_name || ctx.pool;
                collectionDisplay = `<div class="job-collection-cell" style="cursor:pointer;" title="Pool UUID: ${escapeAttr(ctx.pool)}"><i class="fa-solid fa-sitemap"></i> <a onclick="window.Nav && window.Nav.openPath(${escapeAttr(jsString('/pools/' + encodeURIComponent(ctx.pool)))});">${escapeHtml(displayName)}</a></div>`;
            } else {
                collectionDisplay = `<div class="job-collection-cell" style="cursor:pointer;"><i class="fa-solid fa-layer-group"></i> <a onclick="window.Nav && window.Nav.openPath(window.Nav.buildUIUrl(${escapeAttr(jsString(job.collection))}, []));">${escapeHtml(job.collection)}</a></div>`;
            }
        } else {
            collectionDisplay = '<span class="dim">-</span>';
        }

        const targetDisplay = getJobTargetLink(job);
        const durationHtml = window.formatDuration ? window.formatDuration(job.created_at, job.updated_at, status) : '-';
        const rowStyle = shouldHide ? 'display: none;' : '';

        return `
            <tr class="job-row ${isPipeline ? 'pipeline-row' : ''} ${depth > 0 ? 'child-row' : ''} ${depthClass}" ${job.parent_id ? `data-parent-id="${job.parent_id}"` : ''} style="${rowStyle}">
                <td>${firstCellHtml}</td>
                <td>${collectionDisplay}</td>
                <td>${targetDisplay}</td>
                <td>${statusBadge}</td>
                <td>${progressHtml}</td>
                <td class="job-date-cell">${createdDate}</td>
                <td class="job-duration-cell">${durationHtml}</td>
                <td>${actions}</td>
            </tr>
        `;
    }

    function renderTree(item, parentId = null, level = 0, prefix = '', isLast = true, ancestorCollapsed = false) {
        const amICollapsed = collapsedPipelines.has(item.id);
        const shouldHide = (parentId !== null && collapsedPipelines.has(parentId)) || ancestorCollapsed;

        rows.push(renderJobRow(item, prefix, isLast, level, shouldHide));

        if (item.type === 'pipeline' || item.type === 'group') {
            const children = pipelineChildren.get(item.id) || [];
            const nextPrefix = prefix + (level === 0 ? '' : (isLast ? '   ' : '│  '));
            children.forEach((child, i) => {
                const childIsLast = (i === children.length - 1);
                renderTree(child, item.id, level + 1, nextPrefix, childIsLast, shouldHide || amICollapsed);
            });
        }
    }

    topLevelItems.forEach(item => {
        renderTree(item);
    });

    return rows.join('');
}

window.renderJobs = renderJobs;

async function setJobPaused(jobId, paused) {
    try {
        const resp = await fetch(`/api/jobs/${jobId}/pause`, { method: paused ? 'POST' : 'DELETE' });
        if (resp.ok) {
            if (window.refreshData) window.refreshData();
        } else {
            const data = await resp.json();
            alert(`Failed to ${paused ? 'pause' : 'resume'} job: ` + (data.error || 'Unknown error'));
        }
    } catch (e) {
        console.error(e);
        alert(`Error ${paused ? 'pausing' : 'resuming'} job`);
    }
}

window.pauseJob = (jobId) => setJobPaused(jobId, true);
window.resumeJob = (jobId) => setJobPaused(jobId, false);

window.cancelJob = async function (jobId) {
    try {
        const resp = await fetch(`/api/jobs/${jobId}/cancel`, { method: 'POST' });
        if (resp.ok) {
            if (window.refreshData) window.refreshData();
        } else {
            const data = await resp.json();
            alert('Failed to cancel job: ' + (data.error || 'Unknown error'));
        }
    } catch (e) {
        console.error(e);
        alert('Error cancelling job');
    }
};

window.retryJob = async function (jobId) {
    try {
        const resp = await fetch(`/api/jobs/${jobId}/retry`, { method: 'POST' });
        if (resp.ok) {
            if (window.refreshData) window.refreshData();
        } else {
            const data = await resp.json();
            alert('Failed to retry job: ' + (data.error || 'Unknown error'));
        }
    } catch (e) {
        console.error(e);
        alert('Error retrying job');
    }
};

let currentActiveJobId = null;

window.showJobDetails = async function (jobId) {
    currentActiveJobId = jobId;
    await refreshJobModal(jobId, true);
    document.getElementById('job-details-modal').style.display = 'flex';
};

async function refreshJobModal(jobId, isInitial = false) {
    try {
        const resp = await fetch(`/api/jobs/${jobId}`);
        if (!resp.ok) {
            throw new Error(`API error: ${resp.status} ${resp.statusText}`);
        }
        const job = await resp.json();

        if (currentActiveJobId !== jobId) return;

        document.getElementById('job-modal-title').innerText = `Job Details: ${job.type || 'Unknown'}`;

        // Build subtasks HTML
        let subtasksHtml = '';
        if (job.sub_tasks && job.sub_tasks.length > 0) {
            subtasksHtml = `
                <div style="margin-top: 20px;">
                    <h4 style="margin-bottom: 10px; font-size: 0.9rem; color: var(--accent);">Pipeline Sub-tasks</h4>
                    <table class="data-table" style="width:100%; font-size: 0.85rem;">
                        <thead>
                            <tr style="background: var(--hover);">
                                <th style="padding: 8px;">Type</th>
                                <th style="padding: 8px;">Status</th>
                                <th style="padding: 8px;">Progress</th>
                            </tr>
                        </thead>
                        <tbody>
            `;

            job.sub_tasks.forEach(st => {
                let sColor = 'var(--dim)';
                if (st.status === 'completed') sColor = 'var(--success)';
                if (st.status === 'failed') sColor = 'var(--danger)';
                if (st.status === 'running') sColor = 'var(--warning)';

                subtasksHtml += `
                    <tr>
                        <td style="padding: 8px;">${st.type}</td>
                        <td style="padding: 8px;"><span style="color: ${sColor}">${(st.status || 'pending').toUpperCase()}</span></td>
                        <td style="padding: 8px;">
                            <div style="background: var(--hover); height: 6px; width: 100%; border-radius:3px; overflow:hidden;">
                                <div style="background: var(--accent); width: ${st.progress || 0}%; height: 100%;"></div>
                            </div>
                        </td>
                    </tr>
                `;
            });
            subtasksHtml += '</tbody></table></div>';
        }

        // Build Logs HTML
        const logContainerId = 'job-log-viewer';
        let logsInnerHtml = '';

        if (job.logs && job.logs.length > 0) {
            const sortedLogs = [...job.logs].reverse();
            sortedLogs.forEach(log => {
                let htmlLine = '';
                // Try to match [timestamp] message
                const match = log.match(/^\[(\d+)\] (.*)/);
                if (match) {
                    const timestamp = parseInt(match[1]);
                    const message = match[2];
                    const dateStr = formatDate(timestamp);
                    const escapedMessage = message.replace(/</g, "&lt;").replace(/>/g, "&gt;");
                    htmlLine = `<span style="color: var(--accent); opacity: 0.8; font-weight: 500;">[${dateStr}]</span> ${escapedMessage}`;
                } else {
                    htmlLine = log.replace(/</g, "&lt;").replace(/>/g, "&gt;");
                }
                logsInnerHtml += `<div style="margin-bottom: 4px; border-bottom: 1px solid var(--border); padding-bottom: 2px;">${htmlLine}</div>`;
            });
        } else {
            logsInnerHtml = '<div style="font-style: italic;">No logs available yet.</div>';
        }

        let logsHtml = `
            <div style="margin-top: 20px;">
                <h4 style="margin-bottom: 10px; font-size: 0.9rem; color: var(--accent);">Execution Logs</h4>
                <div id="${logContainerId}" style="background: var(--bg); color: var(--subtle); font-family: var(--mono); font-size: 0.75rem; padding: 15px; max-height: 300px; overflow-y: auto; border: 1px solid var(--border); border-radius: 4px; line-height: 1.5;">
                    ${logsInnerHtml}
                </div>
            </div>`;

        // Error message if any
        let errorHtml = '';
        if (job.error) {
            errorHtml = `
                <div style="background: rgba(255, 85, 85, 0.1); border-left: 3px solid var(--danger); padding: 12px; margin: 15px 0; color: #ff8888; font-size: 0.9rem;">
                    <div style="font-weight: bold; margin-bottom: 4px;">Error Details</div>
                    <div style="font-family: var(--mono); font-size: 0.8rem;">${job.error}</div>
                </div>
            `;
        }

        // Build Payload Metadata HTML
        let payloadHtml = '';
        if (job.payload && Object.keys(job.payload).length > 0) {
            payloadHtml = `
                <div style="margin-top: 20px;">
                    <h4 style="margin-bottom: 10px; font-size: 0.9rem; color: var(--accent);">Job Parameters & Metadata</h4>
                    <div style="background: var(--hover); padding: 15px; border-radius: 6px; border: 1px solid var(--border); display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 15px;">
            `;

            for (const [key, value] of Object.entries(job.payload)) {
                let displayValue = value;
                const isNavigable = key === 'collection' && typeof value === 'string' && value !== '' && !job.payload.pool_id;
                const isPoolValue = key === 'pool_id' && typeof value === 'string' && value !== '';

                if (isNavigable) {
                    displayValue = `<a style="cursor:pointer; color: var(--accent); font-size: 0.85rem;" onclick="window.Nav && window.Nav.openPath(window.Nav.buildUIUrl(${escapeAttr(jsString(value))}, []));">${escapeHtml(value)}</a>`;
                } else if (isPoolValue) {
                    displayValue = `<a style="cursor:pointer; color: var(--accent); font-size: 0.85rem;" onclick="void(0); window.Nav && window.Nav.openPath(${escapeAttr(jsString('/pools/' + encodeURIComponent(value)))});">${escapeHtml(value)}</a>`;
                } else if (typeof value === 'object' && value !== null) {
                    displayValue = `<code style="font-size: 0.7rem; color: var(--subtle);">${escapeHtml(JSON.stringify(value))}</code>`;
                } else if (typeof value === 'string' && value.length > 30) {
                    displayValue = `<code title="${value}" style="font-size: 0.75rem;">${value.substring(0, 12)}...${value.substring(value.length - 8)}</code>`;
                } else {
                    displayValue = `<code style="font-size: 0.85rem; color: var(--meta-text);">${value}</code>`;
                }

                payloadHtml += `
                    <div>
                        <div style="color: var(--dim); font-size: 0.65rem; text-transform: uppercase; margin-bottom: 2px; letter-spacing: 0.5px;">${key.replace(/_/g, ' ')}</div>
                        <div style="word-break: break-all;">${displayValue}</div>
                    </div>
                `;
            }
            payloadHtml += '</div></div>';
        }

        const modalBody = document.getElementById('job-modal-body');

        // Try to preserve scroll position of log viewer
        const oldLogViewer = document.getElementById(logContainerId);
        const wasAtBottom = oldLogViewer ? (oldLogViewer.scrollHeight - oldLogViewer.scrollTop <= oldLogViewer.clientHeight + 10) : true;

        modalBody.innerHTML = `
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; background: var(--hover); padding: 15px; border-radius: 6px; border: 1px solid var(--border);">
                <div>
                    <div style="color: var(--dim); font-size: 0.75rem; text-transform: uppercase;">Job ID</div>
                    <div style="font-family: var(--mono); font-size: 0.85rem;">${job.id || ''}</div>
                </div>
                <div>
                    <div style="color: var(--dim); font-size: 0.75rem; text-transform: uppercase;">Status</div>
                    <div style="font-weight: bold; color: var(--accent);">${(job.status || 'unknown').toUpperCase()}</div>
                </div>
            </div>
            ${errorHtml}
            ${payloadHtml}
            ${subtasksHtml}
            ${logsHtml}
        `;

        // Restore scroll or scroll to bottom if it was already at bottom
        const newLogViewer = document.getElementById(logContainerId);
        if (newLogViewer && wasAtBottom) {
            newLogViewer.scrollTop = newLogViewer.scrollHeight;
        }

    } catch (e) {
        console.error(e);
        if (isInitial) alert('Error fetching job details: ' + e.message);
    }
}

window.closeJobModal = function () {
    document.getElementById('job-details-modal').style.display = 'none';
    currentActiveJobId = null;
};

// Fleet-wide pause toggle (global, not per-job — the API has no per-job pause)
let jobsPaused = null;

window.refreshPauseButton = async function (state) {
    const btn = document.getElementById('job-pause-toggle');
    if (!btn) return;
    if (state === undefined) {
        try {
            state = (await (await fetch('/api/jobs/pause')).json()).paused;
        } catch (e) {
            return;
        }
    }
    // Always rewrite: the settings bar re-renders the button as a placeholder,
    // so a memoised "unchanged" skip would leave it stuck blank.
    jobsPaused = state;
    btn.innerHTML = state
        ? '<i class="fa-solid fa-play"></i> Resume Workers'
        : '<i class="fa-solid fa-pause"></i> Pause Workers';
    btn.classList.toggle('active', !!state);
};

window.toggleJobPause = async function () {
    const btn = document.getElementById('job-pause-toggle');
    if (btn) btn.disabled = true;
    try {
        const resp = await fetch('/api/jobs/pause', { method: jobsPaused ? 'DELETE' : 'POST' });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || 'Unknown error');
        refreshPauseButton(data.paused);
    } catch (e) {
        alert('Failed to toggle pause: ' + e.message);
    } finally {
        if (btn) btn.disabled = false;
    }
};

// Modal and auto-refresh setups are completed below

// Auto-refresh when in jobs view — skip when tab is hidden
setInterval(() => {
    if (document.visibilityState !== 'visible') return;
    const restful = (typeof parseRestfulPath === 'function') ? parseRestfulPath() : null;
    const isJobsView = (restful && restful.view === 'jobs') || window.location.pathname === '/jobs' || (window.location.hash && window.location.hash.split('?')[0] === '#jobs');
    if (isJobsView) {
        refreshPauseButton();
        const modal = document.getElementById('job-details-modal');
        const isModalOpen = modal && modal.style.display !== 'none';

        if (isModalOpen && currentActiveJobId) {
            refreshJobModal(currentActiveJobId);
        } else {
            if (localStorage.getItem('jobAutoRefresh') !== 'false') {
                if (window.refreshData) window.refreshData(false, false);
            }
        }
    }
}, 2000);
