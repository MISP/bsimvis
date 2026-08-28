/**
 * Job Status Store -- single source of truth for job state (job-system-
 * rework-plan.md §3.1). One SSE connection per distinct scope actually in
 * use, ref-counted so e.g. two widgets both watching the global scope share
 * one connection. Views subscribe instead of running their own
 * setInterval/fetch loop or hand-rolled poll-until-terminal-then-toast flow.
 *
 * Usage:
 *   const unsub = JobStatusStore.subscribe({collection: 'foo'}, (evt) => {
 *     // evt.type: 'job:sync' | 'job:started' | 'job:progress' |
 *     //           'job:completed' | 'job:failed'
 *     // evt.jobId, evt.data (the job dict), evt.jobs (full current list,
 *     // 'job:sync' only)
 *   });
 *   // later: unsub();
 *
 * Scopes: {}  (global), {collection}, {pool}, {status}, {md5} -- any
 * combination, passed straight through to GET /api/jobs/stream's query
 * params -- or {jobId} for one job's own tail (GET /api/jobs/<id>/stream).
 */
(function () {
    const TERMINAL = new Set(['completed', 'failed', 'cancelled']);

    function scopeKey(scope) {
        scope = scope || {};
        if (scope.jobId) return `job:${scope.jobId}`;
        return JSON.stringify(
            ['collection', 'pool', 'status', 'type', 'md5']
                .filter((k) => scope[k])
                .sort()
                .map((k) => [k, scope[k]])
        );
    }

    function buildStreamUrl(scope) {
        const params = new URLSearchParams();
        ['collection', 'pool', 'status', 'type', 'md5'].forEach((k) => {
            if (scope[k]) params.set(k, scope[k]);
        });
        const qs = params.toString();
        return `/api/jobs/stream${qs ? '?' + qs : ''}`;
    }

    class Entry {
        constructor() {
            this.jobs = new Map();
            this.listeners = new Set();
            this.refCount = 0;
            this.es = null;
        }
        emit(evt) {
            this.listeners.forEach((cb) => {
                try {
                    cb(evt);
                } catch (e) {
                    console.error('[JobStatusStore] listener error', e);
                }
            });
        }
    }

    const scopes = new Map();

    function connectScoped(scope, entry) {
        entry.es = new EventSource(buildStreamUrl(scope));
        entry.es.addEventListener('job', (ev) => {
            let payload;
            try {
                payload = JSON.parse(ev.data);
            } catch (e) {
                return;
            }
            const job = payload.data;
            if (!job || !job.id) return;
            const isNew = !entry.jobs.has(job.id);
            entry.jobs.set(job.id, job);
            let type = 'job:progress';
            if (isNew) type = 'job:started';
            else if (TERMINAL.has(job.status))
                type = job.status === 'completed' ? 'job:completed' : 'job:failed';
            entry.emit({ type, jobId: job.id, data: job });
        });
    }

    function connectJob(jobId, entry) {
        // Seed with the full job doc immediately so a subscriber's first
        // render has every field, not just whatever the log stream mentions.
        fetch(`/api/jobs/${encodeURIComponent(jobId)}`)
            .then((res) => (res.ok ? res.json() : null))
            .then((job) => {
                if (!job || job.error) return;
                entry.jobs.set(jobId, job);
                entry.emit({ type: 'job:started', jobId, data: job });
            })
            .catch(() => {});

        entry.es = new EventSource(`/api/jobs/${encodeURIComponent(jobId)}/stream`);
        entry.es.addEventListener('log', (ev) => {
            let payload;
            try {
                payload = JSON.parse(ev.data);
            } catch (e) {
                return;
            }
            const fields = payload.data || {};
            const prev = entry.jobs.get(jobId) || { id: jobId, status: 'running' };
            const job = Object.assign({}, prev);
            if (fields.processed !== undefined) job.processed_items = Number(fields.processed);
            if (fields.total !== undefined) job.total_items = Number(fields.total);
            if (job.processed_items != null && job.total_items) {
                job.progress = Math.round((100 * job.processed_items) / job.total_items);
            }
            if (fields.phase !== undefined) job.phase = fields.phase;
            if (fields.speed_current !== undefined) job.speed_current = Number(fields.speed_current);
            if (fields.speed_avg !== undefined) job.speed_avg = Number(fields.speed_avg);
            entry.jobs.set(jobId, job);
            entry.emit({ type: 'job:progress', jobId, data: job, log: fields });
        });
        entry.es.addEventListener('done', (ev) => {
            let payload;
            try {
                payload = JSON.parse(ev.data);
            } catch (e) {
                payload = { data: {} };
            }
            const status = payload.data && payload.data.status;
            const prev = entry.jobs.get(jobId) || { id: jobId };
            const job = Object.assign({}, prev, { status });
            entry.jobs.set(jobId, job);
            entry.emit({
                type: status === 'completed' ? 'job:completed' : 'job:failed',
                jobId,
                data: job,
            });
            if (entry.es) entry.es.close();
        });
    }

    const JobStatusStore = {
        /**
         * @param {{collection?,pool?,status?,type?,md5?,jobId?}} scope
         * @param {(evt: {type:string, jobId:string, data:object}) => void} callback
         * @returns {() => void} unsubscribe
         */
        subscribe(scope, callback) {
            scope = scope || {};
            const key = scopeKey(scope);
            let entry = scopes.get(key);
            if (!entry) {
                entry = new Entry();
                scopes.set(key, entry);
                if (scope.jobId) connectJob(scope.jobId, entry);
                else connectScoped(scope, entry);
            }
            entry.listeners.add(callback);
            entry.refCount++;
            // Zero-latency initial read from whatever this scope already knows.
            callback({ type: 'job:sync', jobs: Array.from(entry.jobs.values()) });

            let unsubscribed = false;
            return () => {
                if (unsubscribed) return;
                unsubscribed = true;
                entry.listeners.delete(callback);
                entry.refCount--;
                if (entry.refCount <= 0) {
                    if (entry.es) entry.es.close();
                    scopes.delete(key);
                }
            };
        },

        /** Current known jobs for a scope, without subscribing. */
        snapshot(scope) {
            const entry = scopes.get(scopeKey(scope || {}));
            return entry ? Array.from(entry.jobs.values()) : [];
        },
    };

    window.JobStatusStore = JobStatusStore;
})();
