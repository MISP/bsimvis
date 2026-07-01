import json
from flask import request
from bsimvis.app.services.job_service import JobService

job_service = JobService()


def list_jobs():
    """Lists recent and active jobs with pagination."""
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)
    collection = request.args.get("collection")
    pool = request.args.get("pool")
    status = request.args.get("status")
    jtype = request.args.get("type")
    jobs, total = job_service.list_jobs(
        limit=limit,
        offset=offset,
        collection=collection,
        pool=pool,
        status=status,
        jtype=jtype,
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


def cancel_all_jobs():
    """Cancels all pending or running jobs."""
    cancelled = job_service.cancel_all_jobs()
    return {"status": "cancelled", "cancelled_count": cancelled}


def _reset_job_recursive(job_id):
    """Recursively resets a job and all its sub-tasks/descendants to pending."""
    job = job_service.r.hgetall(f"job:{job_id}")
    if not job:
        return

    # Reset this job
    job_service.r.hset(
        f"job:{job_id}",
        mapping={
            "status": "pending",
            "error": "",
            "progress": 0,
        },
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
