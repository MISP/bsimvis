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
    jobs, total = job_service.list_jobs(
        limit=limit,
        offset=offset,
        collection=collection,
        pool=pool,
        status=status,
        jtype=jtype,
        tier=tier,
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
            "status": "pending",
            "error": "",
            "progress": 0,
        },
    )
    # Delete (not zero) the enqueue + barrier latches: both use field-existence
    # semantics (hset return value), so they must be absent to re-arm on retry.
    job_service.r.hdel(f"job:{job_id}", "queued", "barrier_fired")

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
