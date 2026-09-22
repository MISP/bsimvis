"""Fast scan endpoints: analyse a file, compare it, ingest nothing.

The heavy lifting is in `services/scan_service.py`; this module only turns a
request into a scan job and serves the cached result back. See that module for
why none of this writes to Kvrocks.
"""

import hashlib
import logging

from flask import request

from bsimvis.app.services import archive_service, unpack_service
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.ghidra_lang_service import validate as validate_lang
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.scan_service import (
    RECENT_DEFAULT,
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


def _scan_tree(raw_bytes, file_name, depth=0):
    """Everything worth scanning in one upload: (bytes, name, tag) per file.

    Same shape as `_ingest_tree` (`routes/file.py:886`) with the writes taken
    out: which files get analysed is the handler's call, a packed executable is
    scanned packed and unpacked, a container only through its children.
    """
    options = {
        "password": request.args.get(
            "archive_password", archive_service.DEFAULT_PASSWORD
        )
    }

    handler, children = None, []
    if depth < unpack_service.MAX_DEPTH:
        try:
            handler, children = unpack_service.unpack(raw_bytes, file_name, options)
        except unpack_service.UnpackError as e:
            handler = unpack_service.find_handler(raw_bytes, file_name)
            if handler is None or not handler.parent_is_code:
                raise
            # A packed binary that will not unpack is still a real sample, and
            # the detector is a heuristic that may simply have been wrong.
            logging.warning(f"[-] {file_name}: {handler.name} unpack failed: {e}")
            handler, children = None, []

    tag = handler.tag if handler else None
    leaves = []
    if handler is None or handler.parent_is_code:
        leaves.append((raw_bytes, file_name, tag))
    for child_name, child_bytes in children:
        leaves.extend(_scan_tree(child_bytes, child_name, depth + 1))
    return leaves


def _uploads():
    """Every file posted: the multipart parts, else the raw body as one file."""
    parts = [f for _, f in request.files.items(multi=True)]
    if parts:
        return [(part.read(), part.filename or "unknown") for part in parts]
    return [(request.get_data(), request.args.get("file_name", "unknown"))]


def start_scan():
    """Queue a scan per posted file against the requested scopes."""
    try:
        uploads = _uploads()
        if not any(raw for raw, _ in uploads):
            return {"error": "No data provided"}, 400

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

        # Scan mode runs lean: `scan.modules` rather than the instance's upload
        # defaults, because capa alone can cost more than the rest of the job.
        # A request may still widen it.
        modules = scan_modules(
            request.args.getlist("enable"), request.args.getlist("disable")
        )

        results = []
        for raw_bytes, file_name in uploads:
            body, status = _queue_one(
                scan_service, raw_bytes, file_name, scopes, params, modules, warnings
            )
            body.setdefault("file_name", file_name)
            results.append((body, status))

        if len(results) == 1:
            return results[0]

        # ponytail: max_cached_bytes stays a per-file check, not a batch one --
        # posting 50 files is posting 50 independent scans.
        queued = [body for body, status in results if status == 200]
        return {
            "status": "queued" if queued else "failed",
            "queued": len(queued),
            "scans": [body for body, _ in results],
            "scopes": scopes,
            "modules": modules,
            "warnings": warnings,
        }, (200 if queued else 400)
    except Exception as e:
        logging.error(f"Scan request failed: {e}")
        return {"error": str(e)}, 500


def _queue_one(scan_service, raw_bytes, file_name, scopes, params, modules, warnings):
    """Queue one posted file. Returns (response body, status)."""
    # Checked twice on purpose: once here, before unpacking expands an
    # archive in memory, and again on the whole tree once it is known.
    limit = max_cached_bytes()
    if len(raw_bytes) > limit:
        return {
            "error": f"File is {len(raw_bytes)} bytes, over the "
            f"scan.max_cached_bytes limit of {limit}"
        }, 413

    file_md5 = hashlib.md5(raw_bytes).hexdigest()

    if _flag("unpack", True):
        try:
            leaves = _scan_tree(raw_bytes, file_name)
        except unpack_service.UnpackError as e:
            return {"error": f"Could not extract: {e}"}, 400
    else:
        leaves = [(raw_bytes, file_name, None)]
    if not leaves:
        return {"error": f"{file_name} holds nothing to scan"}, 400

    # The sample, everything unpacked out of it and every feature vector
    # they produce sit in Redis, which is RAM. Refusing is better than
    # swelling it, and it is the whole tree that gets cached.
    cached = len(raw_bytes) + sum(len(b) for b, _, _ in leaves)
    if cached > limit:
        return {
            "error": f"Scanning {file_name} would cache {cached} bytes, over "
            f"the scan.max_cached_bytes limit of {limit}"
        }, 413

    if len(leaves) > 1 or leaves[0][0] is not raw_bytes:
        return (
            _start_container_scan(
                scan_service,
                raw_bytes,
                file_name,
                file_md5,
                leaves,
                scopes,
                params,
                modules,
                warnings,
            ),
            200,
        )

    scan_id = scan_service.create(raw_bytes, file_name, file_md5, scopes, params)
    if warnings:
        scan_service.update(scan_id, warnings=warnings)
    job_id = job_service.create_job(
        JobType.SCAN, _analysis_payload(scan_id, file_md5, file_name, modules)
    )
    scan_service.update(scan_id, job_id=job_id, status="queued")

    return {
        "status": "queued",
        "scan_id": scan_id,
        "job_id": job_id,
        "file_md5": file_md5,
        "scopes": scopes,
        "modules": modules,
        "warnings": warnings,
    }, 200


def _analysis_payload(scan_id, file_md5, file_name, modules):
    """The analysis payload a SCAN job carries, lean by default."""
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
    return payload


def _start_container_scan(
    scan_service,
    raw_bytes,
    file_name,
    file_md5,
    leaves,
    scopes,
    params,
    modules,
    warnings,
):
    """One scan per code leaf, then a container node rolling them up.

    The container is never analysed itself -- an APK has no functions of its
    own -- so its score comes from its children, the way container_sim_service
    scores a stored container.
    """
    children, child_jobs = [], []
    for child_bytes, child_name, child_tag in leaves:
        child_md5 = hashlib.md5(child_bytes).hexdigest()
        child_id = scan_service.create(
            child_bytes, child_name, child_md5, scopes, params
        )
        job_id = job_service.create_job(
            JobType.SCAN,
            _analysis_payload(child_id, child_md5, child_name, modules),
            enqueue=False,
        )
        scan_service.update(
            child_id, job_id=job_id, status="queued", handler_tag=child_tag
        )
        child_jobs.append(job_id)
        children.append(
            {
                "scan_id": child_id,
                "job_id": job_id,
                "file_name": child_name,
                "file_md5": child_md5,
                "handler_tag": child_tag,
            }
        )

    parent_id = scan_service.create(raw_bytes, file_name, file_md5, scopes, params)
    scan_service.update(
        parent_id,
        container=True,
        children=children,
        warnings=warnings,
        status="queued",
    )
    for child in children:
        scan_service.update(child["scan_id"], parent_scan=parent_id)

    pipeline_id = job_service.create_pipeline(
        [
            job_service.create_group(child_jobs, enqueue=False),
            (JobType.SCAN_CONTAINER, {"scan_id": parent_id}),
        ]
    )
    scan_service.update(parent_id, job_id=pipeline_id)

    return {
        "status": "queued",
        "scan_id": parent_id,
        "job_id": pipeline_id,
        "file_md5": file_md5,
        "container": True,
        "children": children,
        "scopes": scopes,
        "modules": modules,
        "warnings": warnings,
    }


def list_scans():
    """The recent scans, newest first, with their current job status."""
    service = get_scan_service()
    docs = service.list_scans(limit=_int_arg("limit") or RECENT_DEFAULT)

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
