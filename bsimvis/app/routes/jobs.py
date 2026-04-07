from flask import Blueprint, jsonify, request
from bsimvis.app.services.job_service import JobService

jobs_bp = Blueprint("jobs", __name__)
job_service = JobService()

@jobs_bp.route("/api/jobs", methods=["GET"])
def list_jobs():
    """Lists recent and active jobs."""
    limit = request.args.get("limit", 50, type=int)
    jobs = job_service.list_jobs(limit=limit)
    return jsonify(jobs)

@jobs_bp.route("/api/jobs/stats", methods=["GET"])
def get_global_stats():
    """Returns aggregate metrics across all jobs."""
    stats = job_service.get_global_stats()
    return jsonify(stats)

@jobs_bp.route("/api/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    """Returns detailed status and logs for a job or pipeline."""
    job = job_service.get_job_status(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)

@jobs_bp.route("/api/jobs/<job_id>/cancel", methods=["POST"])
def cancel_job(job_id):
    """Cancels a pending or running job/pipeline."""
    success = job_service.cancel_job(job_id)
    if not success:
        return jsonify({"error": "Job not found or already completed"}), 404
    return jsonify({"status": "cancelled", "job_id": job_id})
