import json
from flask import request
from bsimvis.app.services.job_service import JobService

job_service = JobService()


def list_jobs():
    """Lists recent and active jobs with pagination."""
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)
    collection = request.args.get("collection")
    pool = request.args.get("pool") or request.args.get("pool_id")

    # If request.args.collection was normalized to global:pool:{pool_id} by the hook, extract pool_id
    if collection and collection.startswith("global:pool:"):
        # e.g., global:pool:5d626a78-e3b6-434f-9855-450734820539
        parts = collection.split(":")
        if len(parts) >= 3 and parts[2]:
            pool = parts[2]
            collection = None

    status = request.args.get("status")
    jtype = request.args.get("type")
    tier = request.args.get("tier", type=int)
    md5 = request.args.get("md5")
    jobs, total = job_service.list_jobs(
        limit=limit,
        offset=offset,
        collection=collection,
        pool=pool,
        status=status,
        jtype=jtype,
        tier=tier,
        md5=md5,
    )
    return {"items": jobs, "total": total}


def get_global_stats():
    """Returns aggregate metrics across all jobs."""
    stats = job_service.get_global_stats()
    return stats


def get_job(job_id):
    """Returns detailed status and logs for a job or pipeline."""
    job = job_service.get_job_status(job_id)
    if not job:
        return {"error": "Job not found"}, 404
    return job


_TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def stream_job(job_id):
    """SSE tail of one job's log_stream (job-system-rework-plan.md §2/§5).

    Pushes every §5 stream entry (log lines and progress/phase checkpoints
    ride the same job_log:<id> Redis Stream) as it's written, then a `done`
    event once the job reaches a terminal status -- the frontend consumes
    this instead of polling GET /api/jobs/<id> in a loop. XREAD BLOCK does
    the waiting server-side so this costs nothing when nothing is happening.
    """
    from flask import Response, stream_with_context

    if not job_service.r.exists(f"job:{job_id}"):
        return {"error": "Job not found"}, 404

    def events():
        key = f"job_log:{job_id}"
        last_id = "0"
        while True:
            resp = job_service.r.xread({key: last_id}, count=100, block=2000)
            got_entries = False
            for _stream_key, items in resp or []:
                for entry_id, fields in items:
                    got_entries = True
                    last_id = entry_id
                    payload = {
                        "id": entry_id,
                        "type": "job.log",
                        "time": fields.get("ts"),
                        "data": fields,
                    }
                    yield f"id: {entry_id}\nevent: log\ndata: {json.dumps(payload)}\n\n"
            if not got_entries:
                status = job_service.r.hget(f"job:{job_id}", "status")
                if status is None or status in _TERMINAL_STATUSES:
                    payload = {"type": "job.done", "data": {"status": status}}
                    yield f"event: done\ndata: {json.dumps(payload)}\n\n"
                    break

    return Response(
        stream_with_context(events()),
        mimetype="text/event-stream",
        # Chunks are useless if a proxy buffers them into one response --
        # same precedent as routes/home.py's unified_search_stream.
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def cancel_job(job_id):
    """Cancels a pending or running job/pipeline."""
    success = job_service.cancel_job(job_id)
    if not success:
        return {"error": "Job not found or already completed"}, 404
    return {"status": "cancelled", "job_id": job_id}


def pause_jobs():
    """Stops workers claiming new jobs. In-flight jobs finish normally."""
    return {"paused": job_service.set_paused(True)}


def resume_jobs():
    """Lets workers claim jobs again."""
    return {"paused": job_service.set_paused(False)}


def get_pause_state():
    return {"paused": job_service.is_paused()}


def pause_job(job_id):
    """Holds one job/group/pipeline back; other jobs keep running."""
    result = job_service.set_job_paused(job_id, True)
    if result is None:
        return {"error": "Job not found"}, 404
    job_service.add_log(job_id, "Paused by user; will not be claimed until resumed.")
    return {"paused": True, "job_id": job_id}


def resume_job(job_id):
    """Releases a paused job/group/pipeline back to the workers."""
    result = job_service.set_job_paused(job_id, False)
    if result is None:
        return {"error": "Job not found"}, 404
    job_service.add_log(job_id, "Resumed by user.")
    return {"paused": False, "job_id": job_id}


def cancel_all_jobs():
    """Cancels all pending or running jobs."""
    cancelled = job_service.cancel_all_jobs()
    return {"status": "cancelled", "cancelled_count": cancelled}


def _reset_job_recursive(job_id):
    """Recursively resets a job and all its sub-tasks/descendants to pending."""
    job = job_service.r.hgetall(f"job:{job_id}")
    if not job:
        return

    # Reset this job.
    job_service.r.hset(
        f"job:{job_id}",
        mapping={
            "error": "",
            "progress": 0,
            # A user-initiated retry is exactly when the lease-expiry counter
            # should start over; without this a job that already burned
            # MAX_ATTEMPTS fails on its first expiry after retry.
            "attempts": 0,
        },
    )
    job_service._set_status(job_id, "pending")
    # Delete (not zero) the enqueue + barrier latches: both use field-existence
    # semantics (hset return value), so they must be absent to re-arm on retry.
    # started_at/completed_at must also go: leaves them stamped from the prior
    # attempt would otherwise report that attempt's duration for this one
    # (job-system-rework-plan.md §3.7).
    job_service.r.hdel(
        f"job:{job_id}", "queued", "barrier_fired", "started_at", "completed_at"
    )

    # Check for children
    task_ids_raw = job.get(b"task_ids") or job.get("task_ids")
    if task_ids_raw:
        try:
            if isinstance(task_ids_raw, bytes):
                task_ids_raw = task_ids_raw.decode()
            task_ids = json.loads(task_ids_raw)
            if isinstance(task_ids, list):
                for tid in task_ids:
                    _reset_job_recursive(tid)
        except Exception:
            pass


def retry_job(job_id):
    """Retries a failed or cancelled job/pipeline/group recursively."""
    job = job_service.get_job_status(job_id)
    if not job:
        return {"error": "Job not found"}, 404

    jtype = job.get("type")

    # Recursively reset the job and all descendants
    _reset_job_recursive(job_id)

    if jtype in ["pipeline", "group"]:
        # Restart via start_job
        job_service.start_job(job_id)
        job_service.add_log(
            job_id,
            f"{jtype.capitalize()} retried by user. Restarting tasks.",
        )
    else:
        # Standard leaf job retry
        job_service.enqueue_job(job_id)
        job_service.add_log(job_id, "Job retried by user.")

    return {"status": "retried", "job_id": job_id}
