from flask import request
from bsimvis.app.services.job_service import JobService

job_service = JobService()


def list_jobs():
    """Lists recent and active jobs with pagination."""
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    jobs, total = job_service.list_jobs(limit=limit, offset=offset)
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


def retry_job(job_id):
    """Retries a failed or cancelled job/pipeline."""
    job = job_service.get_job_status(job_id)
    if not job:
        return {"error": "Job not found"}, 404

    # If it's a pipeline or group
    jtype = job.get("type")
    if jtype in ["pipeline", "group"]:
        task_ids = job.get("task_ids", [])
        if not task_ids:
            return {"error": f"{jtype.capitalize()} has no tasks"}, 400

        # Reset parent metadata
        job_service.r.hset(
            f"job:{job_id}",
            mapping={
                "status": "pending",
                "error": "",
                "progress": 0,
            },
        )

        # Reset all sub-tasks to pending
        for tid in task_ids:
            job_service.r.hset(
                f"job:{tid}", mapping={"status": "pending", "error": "", "progress": 0}
            )

        # Restart via start_job
        job_service.start_job(job_id)
        job_service.add_log(
            job_id,
            f"{jtype.capitalize()} retried by user. Restarting tasks.",
        )
        return {"status": "retried", "job_id": job_id}
    else:
        # Standard job retry
        job_service.r.hset(
            f"job:{job_id}", mapping={"status": "pending", "error": "", "progress": 0}
        )
        job_service.enqueue_job(job_id)
        job_service.add_log(job_id, "Job retried by user.")
        return {"status": "retried", "job_id": job_id}
