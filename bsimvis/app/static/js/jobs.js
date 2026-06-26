/**
 * Jobs UI Module for BSimVis
 */

const collapsedPipelines = new Set(JSON.parse(localStorage.getItem('collapsedPipelines') || '[]'));

window.togglePipelineCollapse = function (pipelineId) {
    if (collapsedPipelines.has(pipelineId)) {
        collapsedPipelines.delete(pipelineId);
    } else {
        collapsedPipelines.add(pipelineId);
    }
    localStorage.setItem('collapsedPipelines', JSON.stringify(Array.from(collapsedPipelines)));
    if (window.refreshData) window.refreshData(false, false);
};

function renderJobs(jobs) {
    const jobsList = Array.isArray(jobs) ? jobs : (jobs.results || []);

    if (jobsList.length === 0) {
        return '<tr><td colspan="9" style="text-align:center; padding: 60px; color: var(--dim);"><i class="fa-solid fa-wind" style="font-size: 2rem; opacity: 0.2; display: block; margin-bottom: 10px;"></i>No recent jobs found</td></tr>';
    }

    const jobsById = new Map();
    const childJobIds = new Set();
    const pipelineChildren = new Map();

    jobsList.forEach(job => {
        jobsById.set(job.id, job);
    });

    // Track children and group them
    jobsList.forEach(job => {
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

    // Order pipeline children according to task_ids sequence
    jobsList.forEach(job => {
        if (job.type === 'pipeline' || job.type === 'group') {
            const children = [];
            const taskIds = job.task_ids || [];
            taskIds.forEach(tid => {
                const childJob = jobsById.get(tid);
                if (childJob) {
                    children.push(childJob);
                }
            });
            // Append any other children not in task_ids list
            const otherChildren = pipelineChildren.get(job.id) || [];
            otherChildren.forEach(child => {
                if (!children.some(c => c.id === child.id)) {
                    children.push(child);
                }
            });
            pipelineChildren.set(job.id, children);
        }
    });

    // Identify top-level items (not child of any existing pipeline in list)
    const topLevelItems = jobsList.filter(job => {
        if (job.parent_id && jobsById.has(job.parent_id)) {
            return false;
        }
        if (childJobIds.has(job.id)) {
            let hasParentInList = false;
            for (let [id, pJob] of jobsById.entries()) {
                if ((pJob.type === 'pipeline' || pJob.type === 'group') && pJob.task_ids && pJob.task_ids.includes(job.id)) {
                    hasParentInList = true;
                    break;
                }
            }
            if (hasParentInList) return false;
        }
        return true;
    });

    // Sort top-level items by created_at desc
    topLevelItems.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

    const rows = [];

    function renderJobRow(job, parentId = null, level = 0, shouldHide = false) {
        const isPipeline = job.type === 'pipeline' || job.type === 'group';
        const progress = job.progress || 0;
        const status = job.status || 'pending';

        let progressClass = '';
        if (status === 'running') progressClass = 'progress-running';
        if (status === 'completed') progressClass = 'progress-completed';
        if (status === 'failed') progressClass = 'progress-failed';
        if (status === 'cancelled') progressClass = 'progress-failed';

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

        const statusBadge = `<span class="job-status-badge status-${status}"><i class="fa-solid ${statusIcon}"></i> ${status.toUpperCase()}</span>`;

        const createdDate = new Date(job.created_at).toLocaleString();

        let actions = '<div class="job-actions">';
        if (status === 'pending' || status === 'running') {
            actions += `<button class="job-btn-action danger" onclick="cancelJob('${job.id}')" title="Cancel Job"><i class="fa-solid fa-ban"></i></button>`;
        }
        if (status === 'failed' || status === 'cancelled' || status === 'completed') {
            actions += `<button class="job-btn-action" onclick="retryJob('${job.id}')" title="Retry/Resume Job"><i class="fa-solid fa-rotate-right"></i></button>`;
        }
        actions += `<button class="job-btn-action info" onclick="showJobDetails('${job.id}')" title="View Logs & Details"><i class="fa-solid fa-circle-info"></i></button>`;
        actions += '</div>';

        const isCollapsed = isPipeline && collapsedPipelines.has(job.id);

        let typeDisplay = '';
        if (isPipeline) {
            const chevron = isCollapsed ? 'fa-chevron-right' : 'fa-chevron-down';
            const icon = job.type === 'pipeline' ? 'fa-microchip' : 'fa-layer-group';
            const labelText = job.type === 'pipeline' ? 'PIPELINE' : 'GROUP';
            typeDisplay = `
                <div class="pipeline-header-cell" onclick="togglePipelineCollapse('${job.id}')" style="cursor: pointer; display: flex; align-items: center; gap: 8px;">
                    <i class="fa-solid ${chevron} collapse-chevron" style="color: var(--accent); width: 12px;"></i>
                    <span class="pipeline-label"><i class="fa-solid ${icon}"></i> ${labelText}</span>
                </div>
            `;
        } else {
            typeDisplay = `<span class="job-label">${job.type}</span>`;
        }

        const indent = level > 0 ? `<span class="job-indent" style="margin-left: ${level * 15}px;"></span>` : '';

        let collectionDisplay;
        if (job.collection) {
            const isPool = job.collection.startsWith('pool:');
            if (isPool) {
                const poolId = job.collection.slice(5);
                collectionDisplay = `<div class="job-collection-cell" style="cursor:pointer;"><i class="fa-solid fa-layer-group"></i> <a onclick="window.Nav && window.Nav.openPath('/pools/${poolId}');"><i class="fa-solid fa-sitemap"></i> ${poolId}</a></div>`;
            } else {
                collectionDisplay = `<div class="job-collection-cell" style="cursor:pointer;"><i class="fa-solid fa-layer-group"></i> <a onclick="window.Nav && window.Nav.openPath(window.Nav.buildUIUrl('${job.collection}', []));">${job.collection}</a></div>`;
            }
        } else {
            collectionDisplay = '<span class="dim">-</span>';
        }

        const targetDisplay = job.target ? `<code class="job-target-text">${job.target}</code>` : '<span class="dim">-</span>';

        const durationHtml = window.formatDuration ? window.formatDuration(job.created_at, job.updated_at, status) : '-';

        const rowStyle = shouldHide ? 'display: none;' : '';

        return `
            <tr class="job-row ${isPipeline ? 'pipeline-row' : ''} ${parentId ? 'child-row' : ''}" ${parentId ? `data-parent-id="${parentId}"` : ''} style="${rowStyle}">
                <td>
                    <div class="job-id-cell">
                        ${indent}
                        <code class="job-id-text" title="${job.id}">${job.id}</code>
                    </div>
                </td>
                <td>
                    <div class="job-type-cell">
                        ${typeDisplay}
                    </div>
                </td>
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

    function renderTree(item, parentId = null, level = 0, ancestorCollapsed = false) {
        const amICollapsed = collapsedPipelines.has(item.id);
        // Hide if the immediate parent is collapsed or any ancestor is collapsed
        const shouldHide = (parentId !== null && collapsedPipelines.has(parentId)) || ancestorCollapsed;

        rows.push(renderJobRow(item, parentId, level, shouldHide));

        if (item.type === 'pipeline' || item.type === 'group') {
            const children = pipelineChildren.get(item.id) || [];
            children.forEach(child => {
                renderTree(child, item.id, level + 1, shouldHide || amICollapsed);
            });
        }
    }

    topLevelItems.forEach(item => {
        renderTree(item);
    });

    return rows.join('');
}

window.renderJobs = renderJobs;

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
        const job = await resp.json();

        if (currentActiveJobId !== jobId) return;

        document.getElementById('job-modal-title').innerText = `Job Details: ${job.type}`;

        // Build subtasks HTML
        let subtasksHtml = '';
        if (job.sub_tasks && job.sub_tasks.length > 0) {
            subtasksHtml = `
                <div style="margin-top: 20px;">
                    <h4 style="margin-bottom: 10px; font-size: 0.9rem; color: var(--accent);">Pipeline Sub-tasks</h4>
                    <table class="data-table" style="width:100%; font-size: 0.85rem;">
                        <thead>
                            <tr style="background: rgba(255,255,255,0.03);">
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
                        <td style="padding: 8px;"><span style="color: ${sColor}">${st.status.toUpperCase()}</span></td>
                        <td style="padding: 8px;">
                            <div style="background: rgba(255,255,255,0.05); height: 6px; width: 100%; border-radius:3px; overflow:hidden;">
                                <div style="background: var(--accent); width: ${st.progress}%; height: 100%;"></div>
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
                logsInnerHtml += `<div style="margin-bottom: 4px; border-bottom: 1px solid rgba(255,255,255,0.02); padding-bottom: 2px;">${htmlLine}</div>`;
            });
        } else {
            logsInnerHtml = '<div style="font-style: italic;">No logs available yet.</div>';
        }

        let logsHtml = `
            <div style="margin-top: 20px;">
                <h4 style="margin-bottom: 10px; font-size: 0.9rem; color: var(--accent);">Execution Logs</h4>
                <div id="${logContainerId}" style="background: #0a0a0a; color: #888; font-family: var(--mono); font-size: 0.75rem; padding: 15px; max-height: 300px; overflow-y: auto; border: 1px solid var(--border); border-radius: 4px; line-height: 1.5;">
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
                    <div style="background: rgba(255,255,255,0.03); padding: 15px; border-radius: 6px; border: 1px solid var(--border); display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 15px;">
            `;

            for (const [key, value] of Object.entries(job.payload)) {
                let displayValue = value;
                const isNavigable = key === 'collection' && typeof value === 'string' && value !== '' && !job.payload.pool_id;
                const isPoolValue = key === 'pool_id' && typeof value === 'string' && value !== '';

                if (isNavigable) {
                    displayValue = `<a style="cursor:pointer; color: var(--accent); font-size: 0.85rem;" onclick="window.Nav && window.Nav.openPath(window.Nav.buildUIUrl('${escapeHtml(value)}', []));">${value}</a>`;
                } else if (isPoolValue) {
                    displayValue = `<a style="cursor:pointer; color: var(--accent); font-size: 0.85rem;" onclick="void(0); window.Nav && window.Nav.openPath('/pools/${value}');">${value}</a>`;
                } else if (typeof value === 'object' && value !== null) {
                    displayValue = `<code style="font-size: 0.7rem; color: var(--subtle);">${JSON.stringify(value)}</code>`;
                } else if (typeof value === 'string' && value.length > 30) {
                    displayValue = `<code title="${value}" style="font-size: 0.75rem;">${value.substring(0, 12)}...${value.substring(value.length - 8)}</code>`;
                } else {
                    displayValue = `<code style="font-size: 0.85rem; color: #eee;">${value}</code>`;
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
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; background: rgba(255,255,255,0.02); padding: 15px; border-radius: 6px; border: 1px solid var(--border);">
                <div>
                    <div style="color: var(--dim); font-size: 0.75rem; text-transform: uppercase;">Job ID</div>
                    <div style="font-family: var(--mono); font-size: 0.85rem;">${job.id}</div>
                </div>
                <div>
                    <div style="color: var(--dim); font-size: 0.75rem; text-transform: uppercase;">Status</div>
                    <div style="font-weight: bold; color: var(--accent);">${job.status.toUpperCase()}</div>
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
        if (isInitial) alert('Error fetching job details');
    }
}

window.closeJobModal = function () {
    document.getElementById('job-details-modal').style.display = 'none';
    currentActiveJobId = null;
};

// Global CSS for Modal if not already present
if (!document.getElementById('jobs-style')) {
    const style = document.createElement('style');
    style.id = 'jobs-style';
    style.textContent = `
        .modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.8);
            backdrop-filter: blur(4px);
            z-index: 20000;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .modal-content {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.5);
            display: flex;
            flex-direction: column;
            max-height: 90vh;
        }
        .modal-header {
            padding: 15px 20px;
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .modal-header h3 { margin: 0; color: var(--accent); }
        .modal-body {
            padding: 20px;
            overflow-y: auto;
            flex: 1;
        }
        .modal-footer {
            padding: 15px 20px;
            border-top: 1px solid var(--border);
            display: flex;
            justify-content: flex-end;
        }
        .close-btn {
            background: none;
            border: none;
            color: var(--dim);
            font-size: 1.5rem;
            cursor: pointer;
            line-height: 1;
        }
        .close-btn:hover { color: var(--text); }
        .job-collection-cell {
            font-size: 0.75rem;
            color: var(--accent);
            background: rgba(255, 171, 46, 0.1);
            padding: 2px 6px;
            border-radius: 4px;
            display: inline-flex;
            align-items: center;
            gap: 5px;
            border: 1px solid rgba(255, 171, 46, 0.2);
        }
        .job-target-text {
            font-family: var(--mono);
            font-size: 0.8rem;
            color: var(--subtle);
        }
        .collapse-chevron {
            cursor: pointer;
            transition: transform 0.2s ease, color 0.2s ease;
        }
        .collapse-chevron:hover {
            color: #fff !important;
        }
        .pipeline-header-cell {
            padding: 4px 8px;
            border-radius: 4px;
            margin-left: -8px;
            transition: background-color 0.2s ease;
        }
        .pipeline-header-cell:hover {
            background: rgba(255, 255, 255, 0.05);
        }
    `;
    document.head.appendChild(style);
}

// Auto-refresh when in jobs view
setInterval(() => {
    const restful = (typeof parseRestfulPath === 'function') ? parseRestfulPath() : null;
    const isJobsView = (restful && restful.view === 'jobs') || window.location.pathname === '/jobs' || (window.location.hash && window.location.hash.split('?')[0] === '#jobs');
    if (isJobsView) {
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
}, 5000);

