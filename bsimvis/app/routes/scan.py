"""Fast scan endpoints: analyse a file, compare it, ingest nothing.

The heavy lifting is in `services/scan_service.py`; this module only turns a
request into a scan job and serves the cached result back. See that module for
why none of this writes to Kvrocks.
"""

import hashlib
import logging

from flask import request

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.ghidra_lang_service import validate as validate_lang
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.scan_service import (
    scan_modules,
    default_top_files,
    get_scan_service,
    max_cached_bytes,
)

job_service = JobService()


def _flag(name, default=False):
    value = request.args.get(name)
    if value is None:
        return default
    return str(value).lower() in ("true", "1", "yes")


def _int_arg(name):
    value = request.args.get(name)
    return int(value) if value not in (None, "") else None


def _float_arg(name):
    value = request.args.get(name)
    return float(value) if value not in (None, "") else None


def start_scan():
    """Queue a scan of the posted bytes against the requested scopes."""
    try:
        raw_bytes = request.get_data()
        if not raw_bytes:
            return {"error": "No data provided"}, 400

        limit = max_cached_bytes()
        if len(raw_bytes) > limit:
            # The sample and every feature vector it produces sit in Redis,
            # which is RAM. Refusing is better than swelling it.
            return {
                "error": f"File is {len(raw_bytes)} bytes, over the "
                f"scan.max_cached_bytes limit of {limit}"
            }, 413

        lang_error = validate_lang(
            request.args.get("processor"), request.args.get("cspec")
        )
        if lang_error:
            return {"error": lang_error}, 400

        scan_service = get_scan_service()
        scopes, warnings = scan_service.resolve_scopes(
            collections=request.args.getlist("collection"),
            pools=request.args.getlist("pool"),
            use_all=_flag("all"),
        )
        if not scopes:
            return {
                "error": "No collection to scan against",
                "warnings": warnings,
            }, 400

        params = {
            "algo": request.args.get("algo"),
            "min_score": _float_arg("min_score"),
            "min_features": _int_arg("min_features"),
            "top_k": _int_arg("top_k"),
            "top_files": _int_arg("top_files") or default_top_files(),
        }

        file_name = request.args.get("file_name", "unknown")
        file_md5 = hashlib.md5(raw_bytes).hexdigest()
        scan_id = scan_service.create(raw_bytes, file_name, file_md5, scopes, params)
        if warnings:
            scan_service.update(scan_id, warnings=warnings)

        # Scan mode runs lean: `scan.modules` rather than the instance's upload
        # defaults, because capa alone can cost more than the rest of the job.
        # A request may still widen it.
        modules = scan_modules(
            request.args.getlist("enable"), request.args.getlist("disable")
        )

        payload = {
            "scan_id": scan_id,
            "file_md5": file_md5,
            "file_name": file_name,
            "profile": request.args.get("profile", "fast"),
            "min_func_len": int(request.args.get("min_func_len", 10)),
            "modules": modules,
            "skip_function_id": "FunctionID" not in modules,
            "skip_capa": "capa" not in modules,
            "skip_yara": "yara" not in modules,
            "skip_rulezet": "rulezet" not in modules,
            "skip_boilerplate": "boilerplate" not in modules,
        }
        for opt in ("processor", "cspec"):
            if opt in request.args:
                payload[opt] = request.args.get(opt)

        job_id = job_service.create_job(JobType.SCAN, payload)
        scan_service.update(scan_id, job_id=job_id, status="queued")

        return {
            "status": "queued",
            "scan_id": scan_id,
            "job_id": job_id,
            "file_md5": file_md5,
            "scopes": scopes,
            "modules": modules,
            "warnings": warnings,
        }
    except Exception as e:
        logging.error(f"Scan request failed: {e}")
        return {"error": str(e)}, 500


def list_scans():
    """List all scans from Redis, resolving their current job statuses."""
    service = get_scan_service()
    docs = service.list_scans()

    # Enrich with job statuses using pipeline
    job_ids = [doc["job_id"] for doc in docs if doc.get("job_id")]
    if job_ids:
        pipe = job_service.r.pipeline()
        for jid in job_ids:
            pipe.hmget(f"job:{jid}", "status", "progress", "error")
        results = pipe.execute()

        job_info = {}
        for jid, res in zip(job_ids, results):
            status, progress, error = res
            job_info[jid] = {"status": status, "progress": progress, "error": error}

        for doc in docs:
            jid = doc.get("job_id")
            if jid and jid in job_info:
                info = job_info[jid]
                if info["status"] is not None:
                    doc["job_status"] = info["status"]
                    doc["progress"] = info["progress"]
                    if info["error"] is not None:
                        doc["error"] = info["error"]

    return {"scans": docs}


def get_scan(scan_id):
    """The scan document: status, warnings and the per-scope summary."""
    doc = get_scan_service().get(scan_id)
    if not doc:
        return {"error": "Scan not found or expired"}, 404

    job = job_service.get_job_status(doc["job_id"]) if doc.get("job_id") else None
    if job:
        doc["job_status"] = job.get("status")
        doc["progress"] = job.get("progress")
        if job.get("error"):
            doc["error"] = job["error"]
    return doc


def get_scan_diff(scan_id):
    """One page of a scored pair's function rows, sorted server-side."""
    collection = request.args.get("collection")
    md5 = request.args.get("md5")
    if not collection or not md5:
        return {"error": "collection and md5 are required"}, 400

    table = request.args.get("table", "matched")
    if table not in ("matched", "unique_to_a", "unique_to_b", "all"):
        return {"error": f"Unknown table '{table}'"}, 400

    page = get_scan_service().rows(
        scan_id,
        collection,
        md5,
        table=table,
        offset=int(request.args.get("offset", 0)),
        limit=min(int(request.args.get("limit", 50)), 500),
        sort_col=request.args.get("sort_col"),
        sort_dir=request.args.get("sort_dir", "desc"),
    )
    if page is None:
        return {"error": "No rows for that scan/collection/md5"}, 404
    return {"scan_id": scan_id, "collection": collection, "md5": md5, **page}


def commit_scan(scan_id):
    """Promote a finished scan into a real collection, without re-analysing."""
    collection = request.args.get("collection")
    if not collection:
        return {"error": "collection is required"}, 400

    result = get_scan_service().commit(
        scan_id,
        collection,
        batch_name=request.args.get("batch_name"),
        skip_sim=_flag("skip_sim"),
        topup=_flag("topup", True),
    )
    return result


def delete_scan(scan_id):
    """Drop a scan's cache before its TTL runs out."""
    if not get_scan_service().delete(scan_id):
        return {"error": "Scan not found or expired"}, 404
    return {"status": "deleted", "scan_id": scan_id}


def scan_defaults():
    """What a scan would do with no arguments. Cheap; the UI reads it once."""
    return {
        "modules": scan_modules(),
        "top_files": default_top_files(),
        "cache_ttl": int(config_service.get("scan.cache_ttl", 86400)),
        "max_cached_bytes": max_cached_bytes(),
    }
