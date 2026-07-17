#!/usr/bin/env python3
"""
BSimVis API Test Suite
======================
Uploads ./data/test/crypto_test, waits for the pipeline to finish,
then exercises every known API endpoint.

Usage:
    uv run python test_api_endpoints.py            # concise summary
    uv run python test_api_endpoints.py -v         # verbose (shows full JSON)
    uv run python test_api_endpoints.py --verbose
"""

import sys
import time
import json
import os
import requests
import uuid

try:
    from dotenv import load_dotenv

    load_dotenv()  # pick up APP_HOST/APP_PORT from .env like the app does
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _default_base_url():
    """Mirror the app's host resolution (bsimvis/cli/main.py)."""
    host = os.getenv("APP_HOST") or "localhost"
    port = os.getenv("APP_PORT") or "5000"
    # APP_HOST is the bind address (often 0.0.0.0); connect via localhost.
    if host in ("0.0.0.0", ""):
        host = "localhost"
    return f"http://{host}:{port}"


BASE_URL = os.getenv("API_URL", _default_base_url())
COLLECTION = f"test_collection_{uuid.uuid4().hex[:8]}"
POOL_ID = f"test_pool_{uuid.uuid4().hex[:8]}"
FILTER_POOL_ID = f"test_pool_{uuid.uuid4().hex[:8]}"
TEST_BINARY = "./data/test/crypto_test"
# Binary similarity needs at least two binaries to produce a pair, so the filter
# step uploads a second one. Same architecture as TEST_BINARY, or every pair
# scores zero and the sort checks compare nothing.
SECOND_BINARY = "./data/test/v01_linux_x64"
POLL_INTERVAL = 3  # seconds between pipeline status polls
POLL_TIMEOUT = 300  # max seconds to wait for pipeline

VERBOSE = "-v" in sys.argv or "--verbose" in sys.argv

# Populated after upload
pipeline_id = None
file_md5 = None
file_md5_2 = None  # second binary, uploaded for the bin_sim filter/sort step
func_id1 = None  # first function found
func_id2 = None  # second function found (for diff)
cluster_id = None
cluster_uuid = None

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
results = []

RESET = "\033[0m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"


def _color(text, color):
    return f"{color}{text}{RESET}"


def vprint(*args, **kwargs):
    """Print only in verbose mode."""
    if VERBOSE:
        print(*args, **kwargs)


def test_endpoint(
    method,
    path,
    params=None,
    data=None,
    files=None,
    raw_body=None,
    headers=None,
    expected_ok=True,
    label=None,
):
    """
    Execute a single API call, record and print the result.

    Returns the parsed JSON body (or None on error).
    """
    url = f"{BASE_URL}{path}"
    display = label or f"{method} {path}"
    body = None
    status = None

    try:
        kwargs = {"timeout": 30}
        if headers:
            kwargs["headers"] = headers
        if params:
            kwargs["params"] = params

        if method == "GET":
            resp = requests.get(url, **kwargs)
        elif method == "POST":
            if raw_body is not None:
                kwargs["data"] = raw_body
                resp = requests.post(url, **kwargs)
            elif files:
                resp = requests.post(url, files=files, **kwargs)
            else:
                resp = requests.post(url, json=data, **kwargs)
        elif method == "PATCH":
            resp = requests.patch(url, json=data, **kwargs)
        elif method == "PUT":
            resp = requests.put(url, json=data, **kwargs)
        elif method == "DELETE":
            resp = requests.delete(url, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")

        status = resp.status_code
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:500] if resp.text else None

        ok = (200 <= status < 300) if expected_ok else True
        success = ok

    except Exception as exc:
        status = "ERROR"
        body = str(exc)
        success = False

    # Record
    results.append(
        {
            "label": display,
            "method": method,
            "path": path,
            "params": params,
            "status": status,
            "success": success,
            "body_preview": (json.dumps(body)[:120] if body else ""),
        }
    )

    # Print
    icon = _color("✔", GREEN) if success else _color("✗", RED)
    scode = _color(str(status), GREEN if success else RED)
    print(f"  {icon}  {BOLD}{display}{RESET}  [{scode}]")

    if VERBOSE and body is not None:
        pretty = (
            json.dumps(body, indent=2) if isinstance(body, (dict, list)) else str(body)
        )
        # Truncate extremely long responses
        if len(pretty) > 2000:
            pretty = pretty[:2000] + f"\n{DIM}  ... (truncated){RESET}"
        for line in pretty.splitlines():
            print(f"     {DIM}{line}{RESET}")

    return body


# ---------------------------------------------------------------------------
# Step 1 – Upload the binary and get pipeline_id / file_md5
# ---------------------------------------------------------------------------
def upload_and_start():
    global pipeline_id, file_md5

    if not os.path.isfile(TEST_BINARY):
        print(
            _color(
                f"\n[SKIP] Binary not found at {TEST_BINARY} – upload skipped.", YELLOW
            )
        )
        print(
            _color(
                "       Set TEST_BINARY or place the file to enable the full test.", DIM
            )
        )
        return False

    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 1 – Upload binary", BOLD))
    print(_color(f"{'='*60}", CYAN))

    with open(TEST_BINARY, "rb") as fh:
        raw = fh.read()

    file_name = os.path.basename(TEST_BINARY)
    params = {
        "collection": COLLECTION,
        "file_name": file_name,
        "batch_name": "API Test Batch",
        "profile": "fast",
        "min_func_len": 10,
        "skip_sim": "false",
    }

    body = test_endpoint(
        "POST",
        "/api/file/upload",
        params=params,
        raw_body=raw,
        headers={"Content-Type": "application/octet-stream"},
        label=f"POST /api/file/upload  ({file_name})",
    )

    if not body:
        return False

    pipeline_id = body.get("pipeline_id")
    file_md5 = body.get("file_md5")
    vprint(f"\n     pipeline_id = {pipeline_id}")
    vprint(f"     file_md5    = {file_md5}")
    return True


# ---------------------------------------------------------------------------
# Step 2 – Poll until the pipeline finishes
# ---------------------------------------------------------------------------
def wait_for_pipeline(job_id=None, banner=" STEP 2 – Wait for pipeline to finish"):
    """Polls a job to completion. Defaults to the upload pipeline; later steps
    pass their own job_id (pool build, bin_sim build) to reuse the poll loop."""
    job_id = job_id or pipeline_id
    if not job_id:
        print(_color("\n[SKIP] No pipeline_id – skipping wait.", YELLOW))
        return False

    print(_color(f"\n{'='*60}", CYAN))
    print(_color(banner, BOLD))
    print(_color(f"{'='*60}", CYAN))
    print(f"  Polling pipeline {_color(job_id, BOLD)} (max {POLL_TIMEOUT}s) …")

    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        try:
            resp = requests.get(f"{BASE_URL}/api/jobs/{job_id}", timeout=10)
            if resp.status_code == 200:
                job = resp.json()
                status = job.get("status", "unknown")
                progress = job.get("progress", 0)
                # Safely format progress as a percentage if numeric
                try:
                    progress_val = float(progress)
                    progress_str = f"{progress_val:.0%}"
                except Exception:
                    progress_str = str(progress)
                print(
                    f"     status={_color(status, YELLOW)}  progress={progress_str}",
                    end="\r",
                )
                # Normalize status for comparison (case‑insensitive)
                status_str = str(status).lower()
                if status_str in ("completed", "failed", "cancelled"):
                    print()  # newline after \r
                    icon = (
                        _color("✔", GREEN)
                        if status_str == "completed"
                        else _color("✗", RED)
                    )
                    print(
                        f"  {icon}  Pipeline {_color(status_str, GREEN if status_str == 'completed' else RED)}"
                    )
                    if VERBOSE:
                        vprint(f"\n  Pipeline details:")
                        vprint(json.dumps(job, indent=2))
                    return status == "completed"
        except Exception as exc:
            vprint(f"\n     Poll error: {exc}")

        time.sleep(POLL_INTERVAL)

    print(_color("\n  [TIMEOUT] Pipeline did not complete in time.", RED))
    return False


# ---------------------------------------------------------------------------
# Step 2b – Test duplicate upload (should fail)
# ---------------------------------------------------------------------------
def test_duplicate_upload():
    if not file_md5:
        return

    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 2b – Test duplicate upload (should fail)", BOLD))
    print(_color(f"{'='*60}", CYAN))

    if not os.path.isfile(TEST_BINARY):
        return

    with open(TEST_BINARY, "rb") as fh:
        raw = fh.read()

    file_name = os.path.basename(TEST_BINARY)
    params = {
        "collection": COLLECTION,
        "file_name": file_name,
        "batch_name": "API Test Batch",
        "profile": "fast",
        "min_func_len": 10,
        "skip_sim": "false",
    }

    test_endpoint(
        "POST",
        "/api/file/upload",
        params=params,
        raw_body=raw,
        headers={"Content-Type": "application/octet-stream"},
        expected_ok=False,
        label=f"POST /api/file/upload [Duplicate] (Expected 400)",
    )


# ---------------------------------------------------------------------------
# Step 2c – Upload a second binary
#
# Binary similarity compares pairs, so a one-file collection produces no bin_sim
# docs at all and every check in step 3c would vacuously pass.
# ---------------------------------------------------------------------------
def upload_second_binary():
    global file_md5_2

    if not file_md5 or not os.path.isfile(SECOND_BINARY):
        print(
            _color(
                f"\n[SKIP] {SECOND_BINARY} not found – bin_sim needs a second binary.",
                YELLOW,
            )
        )
        return False

    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 2c – Upload second binary (for bin_sim pairs)", BOLD))
    print(_color(f"{'='*60}", CYAN))

    with open(SECOND_BINARY, "rb") as fh:
        raw = fh.read()

    body = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            "collection": COLLECTION,
            "file_name": os.path.basename(SECOND_BINARY),
            "batch_name": "API Test Batch 2",
            "profile": "fast",
            "min_func_len": 10,
            "skip_sim": "false",
        },
        raw_body=raw,
        headers={"Content-Type": "application/octet-stream"},
        label=f"POST /api/file/upload  ({os.path.basename(SECOND_BINARY)})",
    )
    if not isinstance(body, dict) or not body.get("file_md5"):
        return False

    file_md5_2 = body["file_md5"]
    vprint(f"     file_md5_2 = {file_md5_2}")
    return wait_for_pipeline(
        body.get("pipeline_id"), banner=" STEP 2c – Wait for second binary pipeline"
    )


# ---------------------------------------------------------------------------
# Step 3 – Resolve IDs needed by downstream tests
# ---------------------------------------------------------------------------
def resolve_ids():
    global func_id1, func_id2, cluster_id, cluster_uuid

    if not file_md5:
        # Use placeholders for demo (allow the script to run without upload)
        func_id1 = f"{COLLECTION}:func:PLACEHOLDER_MD5:00100000"
        func_id2 = f"{COLLECTION}:func:PLACEHOLDER_MD5:00100100"
        return

    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 3 – Resolve function / cluster IDs", BOLD))
    print(_color(f"{'='*60}", CYAN))

    # Fetch first two functions from the uploaded file
    resp = requests.get(
        f"{BASE_URL}/api/function/search",
        params={"collection": COLLECTION, "file_md5": file_md5, "limit": 2},
        timeout=20,
    )
    if resp.status_code == 200:
        funcs = resp.json().get("functions", [])
        if len(funcs) >= 1:
            f = funcs[0]
            func_id1 = f.get("function_id") or (
                f"{COLLECTION}:func:{f.get('file_md5')}:{f.get('entrypoint_address')}"
            )
            vprint(f"     func_id1 = {func_id1}")
        if len(funcs) >= 2:
            f = funcs[1]
            func_id2 = f.get("function_id") or (
                f"{COLLECTION}:func:{f.get('file_md5')}:{f.get('entrypoint_address')}"
            )
            vprint(f"     func_id2 = {func_id2}")

    if not func_id1:
        func_id1 = f"{COLLECTION}:func:{file_md5}:00100000"
    if not func_id2:
        func_id2 = func_id1  # fallback: same func for diff (won't crash the endpoint)

    # Try to get a cluster ID
    resp2 = requests.get(
        f"{BASE_URL}/api/cluster/list",
        params={"collection": COLLECTION, "limit": 1},
        timeout=20,
    )
    if resp2.status_code == 200:
        clusters = resp2.json().get("results", [])
        if clusters:
            cluster_id = clusters[0].get("cluster_id")
            cluster_uuid = clusters[0].get("cluster_uuid")
            vprint(f"     cluster_id   = {cluster_id}")
            vprint(f"     cluster_uuid = {cluster_uuid}")


# ---------------------------------------------------------------------------
# Step 4 – Full endpoint sweep
# ---------------------------------------------------------------------------
def run_all_tests():
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 4 – Testing all endpoints", BOLD))
    print(_color(f"{'='*60}", CYAN))

    # ── Index ──────────────────────────────────────────────────────────────
    print(_color("\n  [Index]", BOLD))
    test_endpoint("GET", "/api/index/status", params={"collection": COLLECTION})
    test_endpoint(
        "GET",
        "/api/index/status",
        params={"collection": COLLECTION, "details": "true"},
        label="GET /api/index/status?details=true",
    )

    # ── Collections ────────────────────────────────────────────────────────
    print(_color("\n  [Collections]", BOLD))
    test_endpoint("GET", "/api/collection/search")
    test_endpoint(
        "GET",
        "/api/collection/search",
        params={"q": COLLECTION},
        label="GET /api/collection/search?q=<name>",
    )

    # ── Jobs ───────────────────────────────────────────────────────────────
    print(_color("\n  [Jobs]", BOLD))
    test_endpoint("GET", "/api/jobs", params={"limit": 10, "offset": 0})
    test_endpoint("GET", "/api/jobs/stats")
    if pipeline_id:
        test_endpoint(
            "GET", f"/api/jobs/{pipeline_id}", label=f"GET /api/jobs/<pipeline_id>"
        )

    # ── Batches ────────────────────────────────────────────────────────────
    print(_color("\n  [Batches]", BOLD))
    test_endpoint("GET", "/api/batch/search", params={"collection": COLLECTION})

    # ── Files ──────────────────────────────────────────────────────────────
    print(_color("\n  [Files]", BOLD))
    test_endpoint("GET", "/api/file/search", params={"collection": COLLECTION})
    test_endpoint(
        "GET",
        "/api/file/search",
        params={
            "collection": COLLECTION,
            "limit": 5,
            "offset": 0,
            "sort_by": "entry_date",
            "sort_order": "desc",
        },
        label="GET /api/file/search (sorted)",
    )
    if file_md5:
        test_endpoint(
            "GET",
            "/api/file/search",
            params={"collection": COLLECTION, "file_md5": file_md5},
            label="GET /api/file/search?file_md5=<md5>",
        )
        test_endpoint(
            "GET",
            "/api/file/call_graph",
            params={"collection": COLLECTION, "file_md5": file_md5},
        )

    # ── Functions ──────────────────────────────────────────────────────────
    print(_color("\n  [Functions]", BOLD))
    test_endpoint(
        "GET", "/api/function/search", params={"collection": COLLECTION, "limit": 5}
    )
    if file_md5:
        test_endpoint(
            "GET",
            "/api/function/search",
            params={"collection": COLLECTION, "file_md5": file_md5, "limit": 5},
            label="GET /api/function/search?file_md5=<md5>",
        )
    test_endpoint(
        "GET",
        "/api/function/search",
        params={
            "collection": COLLECTION,
            "sort_by": "bsim_features_count",
            "sort_order": "desc",
            "limit": 5,
        },
        label="GET /api/function/search (sort by features)",
    )
    if func_id1:
        test_endpoint("GET", "/api/function/code", params={"id": func_id1})
        test_endpoint("GET", "/api/function/features", params={"id": func_id1})
    if func_id1 and func_id2:
        test_endpoint(
            "GET", "/api/function/diff", params={"id1": func_id1, "id2": func_id2}
        )
        test_endpoint(
            "GET",
            "/api/diff",
            params={"id1": func_id1, "id2": func_id2},
            label="GET /api/diff (alias)",
        )

    # ── Features (global) ──────────────────────────────────────────────────
    print(_color("\n  [Features – global]", BOLD))
    test_endpoint("GET", "/api/feature/search", params={"collection": COLLECTION})
    test_endpoint(
        "GET",
        "/api/feature/search",
        params={"collection": COLLECTION, "sort": "tf"},
        label="GET /api/feature/search?sort=tf",
    )
    test_endpoint("GET", "/api/features/status", params={"collection": COLLECTION})
    test_endpoint(
        "GET",
        "/api/features/status",
        params={"collection": COLLECTION, "details": "true"},
        label="GET /api/features/status?details=true",
    )
    test_endpoint("GET", "/api/features/files", params={"collection": COLLECTION})

    # ── Search / Autocomplete / Fields ────────────────────────────────────
    print(_color("\n  [Search / Autocomplete]", BOLD))
    test_endpoint(
        "GET",
        "/api/search/autocomplete",
        params={
            "collection": COLLECTION,
            "level": "func",
            "field": "function_name",
            "q": "ma",
        },
    )
    test_endpoint(
        "GET",
        "/api/search/autocomplete",
        params={
            "collection": COLLECTION,
            "level": "file",
            "field": "file_name",
            "q": "",
        },
        label="GET /api/search/autocomplete (file level)",
    )
    test_endpoint(
        "GET",
        "/api/search/fields",
        params={
            "collection": COLLECTION,
            "level": "func",
            "field": ["function_name", "language_id", "return_type"],
        },
    )

    # ── Similarity ─────────────────────────────────────────────────────────
    print(_color("\n  [Similarity]", BOLD))
    test_endpoint(
        "GET",
        "/api/similarity/search",
        params={"collection": COLLECTION, "min_score": 0.95, "limit": 5},
    )
    test_endpoint(
        "GET",
        "/api/similarity/search",
        params={
            "collection": COLLECTION,
            "min_score": 0.80,
            "cross_binary": "true",
            "limit": 5,
        },
        label="GET /api/similarity/search (cross-binary)",
    )
    test_endpoint("GET", "/api/similarity/status", params={"collection": COLLECTION})
    if file_md5:
        test_endpoint(
            "GET",
            "/api/similarity/status",
            params={"collection": COLLECTION, "md5": file_md5},
            label="GET /api/similarity/status?md5=<md5>",
        )
        test_endpoint(
            "GET",
            "/api/similarity/list",
            params={"collection": COLLECTION, "md5": file_md5},
        )
    test_endpoint(
        "GET", "/api/similarity/batches", params={"collection": COLLECTION, "by": "md5"}
    )
    test_endpoint(
        "GET",
        "/api/similarity/batches",
        params={"collection": COLLECTION, "by": "batch"},
        label="GET /api/similarity/batches?by=batch",
    )
    if func_id1 and func_id2 and func_id1 != func_id2:
        test_endpoint(
            "GET", "/api/similarity", params={"id1": func_id1, "id2": func_id2}
        )

    # ── Tags ───────────────────────────────────────────────────────────────
    print(_color("\n  [Tags]", BOLD))
    test_endpoint("GET", "/api/tags", params={"collection": COLLECTION})
    test_endpoint("GET", "/api/tags/metadata", params={"collection": COLLECTION})
    # Add a test tag to a file entity
    if file_md5:
        file_entity_id = f"{COLLECTION}:file:{file_md5}"
        test_endpoint(
            "POST",
            "/api/tags/add",
            data={
                "collection": COLLECTION,
                "entity_type": "file",
                "entity_id": file_entity_id,
                "tag": "test_tag",
            },
            label="POST /api/tags/add (file tag)",
        )
        test_endpoint(
            "GET",
            "/api/tags/stats",
            params={"collection": COLLECTION, "tag": "test_tag"},
        )
        test_endpoint(
            "POST",
            "/api/tags/color",
            data={"collection": COLLECTION, "tag": "test_tag", "color": "#ff6600"},
            label="POST /api/tags/color",
        )
        test_endpoint(
            "POST",
            "/api/tags/priority",
            data={"collection": COLLECTION, "tag": "test_tag", "priority": 1},
            label="POST /api/tags/priority",
        )
        # Bulk add
        test_endpoint(
            "POST",
            "/api/tags/bulk_add",
            data={
                "collection": COLLECTION,
                "entity_type": "file",
                "entity_ids": [file_entity_id],
                "tag": "bulk_test",
            },
            label="POST /api/tags/bulk_add",
        )
        # Remove and bulk remove
        test_endpoint(
            "POST",
            "/api/tags/remove",
            data={
                "collection": COLLECTION,
                "entity_type": "file",
                "entity_id": file_entity_id,
                "tag": "test_tag",
            },
            label="POST /api/tags/remove",
        )
        test_endpoint(
            "POST",
            "/api/tags/bulk_remove",
            data={
                "collection": COLLECTION,
                "entity_type": "file",
                "entity_ids": [file_entity_id],
                "tag": "bulk_test",
            },
            label="POST /api/tags/bulk_remove",
        )

    # ── Clusters ───────────────────────────────────────────────────────────
    print(_color("\n  [Clusters]", BOLD))
    test_endpoint("GET", "/api/cluster/list", params={"collection": COLLECTION})
    test_endpoint(
        "GET",
        "/api/cluster/list",
        params={
            "collection": COLLECTION,
            "sort_by": "stability",
            "sort_order": "desc",
            "min_count": 2,
        },
        label="GET /api/cluster/list (filtered)",
    )
    body_tree = test_endpoint(
        "GET", "/api/cluster/tree", params={"collection": COLLECTION}, expected_ok=False
    )  # may return 404 if no tree yet
    test_endpoint(
        "GET",
        "/api/cluster/dendrogram",
        params={"collection": COLLECTION},
        expected_ok=False,
    )
    if cluster_id:
        test_endpoint(
            "GET",
            "/api/cluster/members",
            params={"collection": COLLECTION, "cluster_id": cluster_id},
        )
    if cluster_uuid:
        test_endpoint(
            "GET",
            "/api/cluster/functions",
            params={"collection": COLLECTION, "cluster_uuid": cluster_uuid},
        )

    # ── Binary Similarity ──────────────────────────────────────────────────
    print(_color("\n  [Binary Similarity]", BOLD))
    test_endpoint("GET", "/api/bin_sim/search", params={"collection": COLLECTION})
    if file_md5:
        test_endpoint(
            "GET",
            "/api/bin_sim/list",
            params={"collection": COLLECTION, "md5": file_md5},
        )

    # ── Binary Clusters ────────────────────────────────────────────────────
    print(_color("\n  [Binary Clusters]", BOLD))
    test_endpoint("GET", "/api/bin_cluster/list", params={"collection": COLLECTION})

    # ── Pools ──────────────────────────────────────────────────────────────
    print(_color("\n  [Pools]", BOLD))
    test_endpoint("GET", "/api/pool", params={"collection": COLLECTION})

    # ── Notes ──────────────────────────────────────────────────────────────
    print(_color("\n  [Notes]", BOLD))
    if func_id1:
        test_endpoint(
            "GET",
            "/api/notes/list",
            params={"collection": COLLECTION, "func_id": func_id1},
            label="GET /api/notes/list?func_id=<id>",
        )
    if file_md5:
        test_endpoint(
            "GET",
            "/api/notes/file/list",
            params={"collection": COLLECTION, "file_id": f"{COLLECTION}:file:{file_md5}"},
            label="GET /api/notes/file/list?file_id=<id>",
        )

    # ── Misc (index config / details) ──────────────────────────────────────
    print(_color("\n  [Misc]", BOLD))
    test_endpoint("GET", "/api/index/config")
    if file_md5:
        test_endpoint(
            "GET",
            f"/api/file/details/{file_md5}",
            params={"collection": COLLECTION},
            label="GET /api/file/details/<md5>",
        )

    # ── Metadata Propagation ───────────────────────────────────────────────
    print(_color("\n  [Metadata Propagation]", BOLD))
    if file_md5:
        patch_body = test_endpoint(
            "PATCH",
            f"/api/file/{file_md5}/metadata",
            data={
                "collection": COLLECTION,
                "metadata": {
                    "yara": ["test_yara_rule_propagate"],
                    "avtype": ["test_avtype_propagate"],
                    "file_names": ["propagated_test_name.exe"],
                },
            },
            label=f"PATCH /api/file/{file_md5}/metadata",
        )
        if patch_body and "job_id" in patch_body:
            job_id = patch_body["job_id"]
            deadline = time.time() + 60
            while time.time() < deadline:
                try:
                    resp = requests.get(f"{BASE_URL}/api/jobs/{job_id}", timeout=10)
                    if resp.status_code == 200:
                        job = resp.json()
                        status = str(job.get("status", "unknown")).lower()
                        if status in ("completed", "failed", "cancelled"):
                            print(f"     propagation job status={status}")
                            break
                except Exception as e:
                    vprint(f"     Poll propagation error: {e}")
                time.sleep(2)

            test_endpoint(
                "GET",
                "/api/file/search",
                params={"collection": COLLECTION, "yara": "test_yara_rule_propagate"},
                label="GET /api/file/search (by propagated yara)",
            )

        bulk_body = test_endpoint(
            "POST",
            "/api/file/metadata/propagate",
            data={
                "collection": COLLECTION,
                "updates": {
                    file_md5: {
                        "yara": ["bulk_yara_rule_propagate"],
                        "avtype": ["bulk_avtype_propagate"],
                    }
                },
            },
            label="POST /api/file/metadata/propagate",
        )
        if bulk_body and "job_id" in bulk_body:
            job_id = bulk_body["job_id"]
            deadline = time.time() + 60
            while time.time() < deadline:
                try:
                    resp = requests.get(f"{BASE_URL}/api/jobs/{job_id}", timeout=10)
                    if resp.status_code == 200:
                        job = resp.json()
                        status = str(job.get("status", "unknown")).lower()
                        if status in ("completed", "failed", "cancelled"):
                            print(f"     bulk propagation job status={status}")
                            break
                except Exception as e:
                    vprint(f"     Poll bulk propagation error: {e}")
                time.sleep(2)

            test_endpoint(
                "GET",
                "/api/file/search",
                params={"collection": COLLECTION, "yara": "bulk_yara_rule_propagate"},
                label="GET /api/file/search (by bulk propagated yara)",
            )

    # ── Collection Clean ───────────────────────────────────────────────────
    print(_color("\n  [Collection Clean]", BOLD))
    clean_body = test_endpoint(
        "POST",
        "/api/collection/clean",
        data={"collection": COLLECTION},
        label="POST /api/collection/clean",
    )
    if clean_body and "job_id" in clean_body:
        job_id = clean_body["job_id"]
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                resp = requests.get(f"{BASE_URL}/api/jobs/{job_id}", timeout=10)
                if resp.status_code == 200:
                    job = resp.json()
                    status = str(job.get("status", "unknown")).lower()
                    if status in ("completed", "failed", "cancelled"):
                        print(f"     clean status={status}")
                        break
            except Exception as e:
                vprint(f"     Poll clean error: {e}")
            time.sleep(2)

    # ── Collection Delete ──────────────────────────────────────────────────
    print(_color("\n  [Collection Delete]", BOLD))
    delete_body = test_endpoint(
        "POST",
        "/api/collection/delete",
        data={"collection": COLLECTION},
        label="POST /api/collection/delete",
    )
    if delete_body and "job_id" in delete_body:
        job_id = delete_body["job_id"]
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                resp = requests.get(f"{BASE_URL}/api/jobs/{job_id}", timeout=10)
                if resp.status_code == 200:
                    job = resp.json()
                    status = str(job.get("status", "unknown")).lower()
                    if status in ("completed", "failed", "cancelled"):
                        print(f"     deletion status={status}")
                        break
            except Exception as e:
                vprint(f"     Poll deletion error: {e}")
            time.sleep(2)


# ---------------------------------------------------------------------------
# Step 3b – Pool <-> collection annotation propagation
#
# Runs BEFORE run_all_tests(): that step ends by calling /api/collection/clean
# and /api/collection/delete, which removes the very docs these checks tag.
#
# Inspired by test_pools.py: builds a pool over the uploaded collection and
# checks the ownership rule from both directions.
#   - tags and notes are OWNED by the origin collection and MIRRORED into every
#     pool containing it, no matter which side wrote them;
#   - clusters are an auto-analysis artifact of whichever namespace computed
#     them, so a pool must never inherit its collections' cluster labels.
# ---------------------------------------------------------------------------
def check(label, condition, detail=""):
    """Record a content assertion in the same results table as test_endpoint()."""
    success = bool(condition)
    results.append(
        {
            "label": label,
            "method": "CHECK",
            "path": "",
            "params": None,
            "status": "OK" if success else "ASSERT",
            "success": success,
            "body_preview": detail,
        }
    )
    icon = _color("✔", GREEN) if success else _color("✗", RED)
    print(f"  {icon}  {BOLD}{label}{RESET}")
    if detail and (VERBOSE or not success):
        print(f"     {DIM}{detail}{RESET}")
    return success


def _search_file_md5s(params):
    """Returns the set of md5s returned by /api/file/search for given params."""
    try:
        resp = requests.get(f"{BASE_URL}/api/file/search", params=params, timeout=30)
        if resp.status_code != 200:
            return set()
        body = resp.json()
        rows = body.get("files", []) if isinstance(body, dict) else body
        if not isinstance(rows, list):
            return set()
        found = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            md5 = row.get("file_md5") or row.get("md5")
            if not md5 and row.get("file_id"):
                # file_id is "{collection}:file:{md5}"
                parts = str(row["file_id"]).split(":")
                md5 = parts[2] if len(parts) >= 3 else None
            if md5:
                found.add(md5)
        return found
    except Exception as exc:
        vprint(f"     file search error: {exc}")
        return set()


def _note_texts(body):
    """Extracts note texts from a /api/notes/list body: {"status":..,"notes":[..]}."""
    notes = body.get("notes", []) if isinstance(body, dict) else body
    if not isinstance(notes, list):
        return []
    return [n.get("text") for n in notes if isinstance(n, dict)]


def test_pool_annotation_propagation():
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 3b – Pool <-> collection tag/note propagation", BOLD))
    print(_color(f"{'='*60}", CYAN))

    if not file_md5:
        print(_color("\n[SKIP] No file_md5 – pool propagation test skipped.", YELLOW))
        return

    from bsimvis.app.services.pool_service import pool_service
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()
    file_entity_id = f"{COLLECTION}:file:{file_md5}"
    added_note_ids = []

    # ── Create a pool over the uploaded collection ─────────────────────────
    # Created through the service, not POST /api/pool, on purpose: the API
    # enqueues a full async pipeline (pool similarity + clustering) that would
    # both race these assertions and compute real pool clusters, which would
    # make the "clusters are not inherited" check below meaningless.
    # build_pool_indexes() is the merge under test, and nothing else.
    print(_color("\n  [Pool setup]", BOLD))
    success, msg = pool_service.create_pool(
        POOL_ID,
        "API Test Propagation Pool",
        [COLLECTION],
        {"only_cross_collection": False},
    )
    if not check(f"pool {POOL_ID} created over {COLLECTION}", success, msg):
        return
    pool_service.build_pool_indexes(POOL_ID)

    try:
        check(
            "pool is registered on the collection (reverse membership)",
            POOL_ID
            in {
                p.decode() if isinstance(p, bytes) else p
                for p in r.smembers(f"{COLLECTION}:pools")
            },
            f"{COLLECTION}:pools",
        )

        # ── Direction 1: tag written in COLLECTION context ─────────────────
        print(_color("\n  [Direction 1: tag added on the collection]", BOLD))
        coll_tag = "prop_from_coll"
        test_endpoint(
            "POST",
            "/api/tags/add",
            data={
                "collection": COLLECTION,
                "entity_type": "file",
                "entity_id": file_entity_id,
                "tag": coll_tag,
            },
            label="POST /api/tags/add (collection context)",
        )
        check(
            "collection-added tag is indexed on the collection",
            r.exists(f"{COLLECTION}:idx:file:user_tags:{coll_tag}"),
            f"{COLLECTION}:idx:file:user_tags:{coll_tag}",
        )
        check(
            "collection-added tag is mirrored into the pool index",
            r.exists(f"global:pool:{POOL_ID}:idx:file:user_tags:{coll_tag}"),
            f"global:pool:{POOL_ID}:idx:file:user_tags:{coll_tag}",
        )
        check(
            "collection-added tag filters files inside the pool",
            file_md5 in _search_file_md5s({"pool": POOL_ID, "user_tag": coll_tag}),
            f"GET /api/file/search?pool={POOL_ID}&user_tag={coll_tag}",
        )

        # ── Direction 2: tag written in POOL context ───────────────────────
        print(_color("\n  [Direction 2: tag added from the pool]", BOLD))
        pool_tag = "prop_from_pool"
        test_endpoint(
            "POST",
            "/api/tags/add",
            data={
                "pool": POOL_ID,
                "entity_type": "file",
                "entity_id": file_entity_id,
                "tag": pool_tag,
            },
            label="POST /api/tags/add (pool context)",
        )
        # The rule: a tag written from a pool is still OWNED by the collection.
        check(
            "pool-added tag is indexed back onto the origin collection",
            r.exists(f"{COLLECTION}:idx:file:user_tags:{pool_tag}"),
            f"{COLLECTION}:idx:file:user_tags:{pool_tag}",
        )
        check(
            "pool-added tag filters files in the collection",
            file_md5
            in _search_file_md5s({"collection": COLLECTION, "user_tag": pool_tag}),
            f"GET /api/file/search?collection={COLLECTION}&user_tag={pool_tag}",
        )
        check(
            "pool-added tag filters files in the pool",
            file_md5 in _search_file_md5s({"pool": POOL_ID, "user_tag": pool_tag}),
            f"GET /api/file/search?pool={POOL_ID}&user_tag={pool_tag}",
        )

        # ── Notes, both directions ─────────────────────────────────────────
        print(_color("\n  [Notes: both directions]", BOLD))
        if func_id1:
            added = test_endpoint(
                "POST",
                "/api/notes/add",
                data={
                    "pool": POOL_ID,
                    "collection": COLLECTION,
                    "func_id": func_id1,
                    "text": "note written from the pool",
                    "owner": "pool_writer",
                },
                label="POST /api/notes/add (pool context)",
            )
            if isinstance(added, dict) and added.get("note"):
                added_note_ids.append(added["note"].get("id"))
            coll_notes = test_endpoint(
                "GET",
                "/api/notes/list",
                params={"collection": COLLECTION, "func_id": func_id1},
                label="GET /api/notes/list (collection context)",
            )
            texts = _note_texts(coll_notes)
            check(
                "pool-added note is readable from the collection",
                "note written from the pool" in texts,
                f"notes seen from collection: {texts}",
            )
            check(
                "pool-added note owner is indexed onto the origin collection",
                r.exists(f"{COLLECTION}:idx:func:note_owners:pool_writer"),
                f"{COLLECTION}:idx:func:note_owners:pool_writer",
            )
            check(
                "pool-added note owner is mirrored into the pool index",
                r.exists(f"global:pool:{POOL_ID}:idx:func:note_owners:pool_writer"),
                f"global:pool:{POOL_ID}:idx:func:note_owners:pool_writer",
            )

            added = test_endpoint(
                "POST",
                "/api/notes/add",
                data={
                    "collection": COLLECTION,
                    "func_id": func_id1,
                    "text": "note written on the collection",
                    "owner": "coll_writer",
                },
                label="POST /api/notes/add (collection context)",
            )
            if isinstance(added, dict) and added.get("note"):
                added_note_ids.append(added["note"].get("id"))
            pool_notes = test_endpoint(
                "GET",
                "/api/notes/list",
                params={"pool": POOL_ID, "func_id": func_id1},
                label="GET /api/notes/list (pool context)",
            )
            pool_texts = _note_texts(pool_notes)
            check(
                "collection-added note is readable from the pool",
                "note written on the collection" in pool_texts,
                f"notes seen from pool: {pool_texts}",
            )
        else:
            print(_color("     [SKIP] no func_id resolved – note checks skipped.", YELLOW))

        # ── Rebuild: mirrors must survive an index rebuild ─────────────────
        # init_pool_build() wipes the pool namespace before merging, so live
        # propagation cannot be what restores these — the merge has to pull them
        # back from the member collections. This is also the only path by which
        # annotations made BEFORE the pool existed ever reach it.
        print(_color("\n  [Pool index rebuild (wipe + merge)]", BOLD))
        pool_service.init_pool_build(POOL_ID)
        for tag in (coll_tag, pool_tag):
            check(
                f"tag '{tag}' survives a pool index rebuild",
                r.exists(f"global:pool:{POOL_ID}:idx:file:user_tags:{tag}"),
                f"global:pool:{POOL_ID}:idx:file:user_tags:{tag}",
            )
        if func_id1:
            check(
                "note owner survives a pool index rebuild",
                r.exists(f"global:pool:{POOL_ID}:idx:func:note_owners:pool_writer"),
                f"global:pool:{POOL_ID}:idx:func:note_owners:pool_writer",
            )
        check(
            "rebuilt pool still filters files by user tag",
            file_md5 in _search_file_md5s({"pool": POOL_ID, "user_tag": coll_tag}),
            f"GET /api/file/search?pool={POOL_ID}&user_tag={coll_tag}",
        )

        # ── Clusters must NOT propagate ────────────────────────────────────
        # This pool was built with build_pool_indexes() only, so pool clustering
        # never ran: any cluster bucket in the pool namespace could only have
        # been inherited from the collection, which is exactly what must not
        # happen. The collection must have clusters for the check to mean
        # anything, so that is asserted first.
        print(_color("\n  [Clusters stay namespace-local]", BOLD))
        for field in ("bin_cluster_id", "cluster_id", "cluster_uuid", "inferred_yara"):
            level = "file" if field.startswith(("bin_cluster", "inferred")) else "func"
            coll_buckets = list(
                r.scan_iter(match=f"{COLLECTION}:idx:{level}:{field}:*", count=100)
            )
            pool_buckets = list(
                r.scan_iter(
                    match=f"global:pool:{POOL_ID}:idx:{level}:{field}:*", count=100
                )
            )
            if not coll_buckets:
                vprint(f"     [skip] collection has no '{field}' buckets to inherit")
                continue
            check(
                f"pool did not inherit '{field}' from the collection",
                not pool_buckets,
                f"collection has {len(coll_buckets)} bucket(s), pool has {len(pool_buckets)}",
            )

    finally:
        # ── Cleanup ────────────────────────────────────────────────────────
        for tag in ("prop_from_coll", "prop_from_pool"):
            try:
                requests.post(
                    f"{BASE_URL}/api/tags/remove",
                    json={
                        "collection": COLLECTION,
                        "entity_type": "file",
                        "entity_id": file_entity_id,
                        "tag": tag,
                    },
                    timeout=30,
                )
            except Exception:
                pass
        for note_id in added_note_ids:
            try:
                requests.delete(
                    f"{BASE_URL}/api/notes/remove",
                    json={
                        "collection": COLLECTION,
                        "func_id": func_id1,
                        "note_id": note_id,
                    },
                    timeout=30,
                )
            except Exception:
                pass
        try:
            pool_service.delete_pool(POOL_ID)
            print(_color(f"\n  Pool {POOL_ID} deleted.", DIM))
        except Exception as exc:
            print(_color(f"\n  Pool cleanup failed: {exc}", YELLOW))


# ---------------------------------------------------------------------------
# Step 3c – Cross-level filtering and sorting, collection vs pool
#
# The levels do not share an index model, so a tag filter is a different query
# at each one, and each has broken independently:
#   - function level indexes the file's tags under file_user_tags;
#   - similarity level is reached by propagation through sim:involves:*;
#   - bin_sim keeps its own denormalized copy of the file's tags, written at
#     build time only.
# Every tag here is therefore added AFTER both builds: a snapshot-based filter
# passes when the tag predates the build and fails afterwards, which is exactly
# the failure the checks have to catch. Pools are checked on both of their
# bin_sim search paths — the O(N) scan used before reindexing, and the
# index-backed path used after.
# ---------------------------------------------------------------------------
FILE_TAG = "filt_file_tag"
FUNC_TAG = "filt_func_tag"


def _search_rows(path, params, key):
    """Returns the row list from a search endpoint, or [] on any failure."""
    try:
        resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=60)
        if resp.status_code != 200:
            vprint(f"     {path} -> HTTP {resp.status_code}")
            return []
        body = resp.json()
        rows = body.get(key, []) if isinstance(body, dict) else body
        return rows if isinstance(rows, list) else []
    except Exception as exc:
        vprint(f"     {path} error: {exc}")
        return []


def _is_sorted(values, order):
    pairs = zip(values, values[1:])
    return all(a >= b for a, b in pairs) if order == "desc" else all(
        a <= b for a, b in pairs
    )


def _tags_of(row, *fields):
    out = set()
    for f in fields:
        v = row.get(f) or []
        out.update(t.lower() for t in v if isinstance(t, str))
    return out


def _check_sorting(label, path, params, key, field):
    """Both orders must be monotonic and agree on the membership of the result."""
    desc = _search_rows(path, dict(params, sort_by=field, sort_order="desc", limit=50), key)
    asc = _search_rows(path, dict(params, sort_by=field, sort_order="asc", limit=50), key)
    if not desc:
        vprint(f"     [skip] {label}: no rows to sort")
        return
    d_vals = [row.get(field) or 0 for row in desc]
    a_vals = [row.get(field) or 0 for row in asc]
    check(f"{label}: sort_by={field} desc is monotonic", _is_sorted(d_vals, "desc"), str(d_vals[:6]))
    check(f"{label}: sort_by={field} asc is monotonic", _is_sorted(a_vals, "asc"), str(a_vals[:6]))
    # A sort must reorder the page, never change which rows are on it.
    check(
        f"{label}: sort_by={field} order does not change the result set",
        sorted(d_vals) == sorted(a_vals),
        f"desc={sorted(d_vals)[:6]} asc={sorted(a_vals)[:6]}",
    )


def _check_namespace_filters(label, ns, bin_sim_expected=True):
    """Runs the filter/sort checks for one namespace ({"collection":..}/{"pool":..})."""
    print(_color(f"\n  [{label}]", BOLD))

    # ── Function level: file tag filters AND is enriched onto the row ──────
    rows = _search_rows(
        "/api/function/search", dict(ns, file_user_tag=FILE_TAG, limit=50), "functions"
    )
    check(f"{label}: function search filters by file_user_tag", bool(rows), f"{len(rows)} row(s)")
    if rows:
        # The bug this catches: the filter hits the func-level index while the
        # row's file_tags are enriched from the file doc, so the two can
        # disagree — searchable but invisible.
        check(
            f"{label}: filtered functions carry file_user_tags on the row",
            all(FILE_TAG in _tags_of(row, "file_user_tags") for row in rows),
            f"first row file_user_tags={rows[0].get('file_user_tags')}",
        )

    rows = _search_rows(
        "/api/function/search", dict(ns, user_tag=FUNC_TAG, limit=50), "functions"
    )
    check(f"{label}: function search filters by func user_tag", bool(rows), f"{len(rows)} row(s)")
    if rows:
        check(
            f"{label}: function tagged on a tagged file also shows its file tags",
            all(FILE_TAG in _tags_of(row, "file_user_tags") for row in rows),
            f"first row file_user_tags={rows[0].get('file_user_tags')}",
        )

    _check_sorting(
        f"{label}: functions", "/api/function/search", ns, "functions", "instruction_count"
    )

    # ── Similarity level: reached only by propagation ──────────────────────
    # min_score is pinned: the endpoint defaults it from config (0.9), so an
    # unpinned filter query and the unfiltered baseline could disagree on the
    # threshold and make a pass or fail meaningless. Note the response key here
    # is "pairs", not "results" like bin_sim.
    sim_params = dict(ns, min_score=0.0, limit=50)
    all_pairs = _search_rows("/api/similarity/search", sim_params, "pairs")
    if not all_pairs:
        vprint(f"     [skip] {label}: no similarity pairs above min_score")
    else:
        rows = _search_rows(
            "/api/similarity/search", dict(sim_params, func_user_tag=FUNC_TAG), "pairs"
        )
        check(
            f"{label}: similarity search filters by func_user_tag",
            bool(rows),
            f"{len(rows)} of {len(all_pairs)} pair(s)",
        )
        rows = _search_rows(
            "/api/similarity/search", dict(sim_params, file_user_tag=FILE_TAG), "pairs"
        )
        check(
            f"{label}: similarity search filters by file_user_tag",
            bool(rows),
            f"{len(rows)} of {len(all_pairs)} pair(s)",
        )
        _check_sorting(
            f"{label}: similarities", "/api/similarity/search", sim_params, "pairs", "score"
        )

    # ── bin_sim level: denormalized copy, tagged after the build ───────────
    if not bin_sim_expected:
        return
    all_pairs = _search_rows("/api/bin_sim/search", dict(ns, limit=50), "results")
    if not all_pairs:
        vprint(f"     [skip] {label}: no bin_sim pairs built")
        return

    rows = _search_rows("/api/bin_sim/search", dict(ns, file_tag=FILE_TAG, limit=50), "results")
    check(
        f"{label}: bin_sim search filters by a file tag added after the build",
        bool(rows),
        f"{len(rows)} of {len(all_pairs)} pair(s)",
    )
    if rows:
        check(
            f"{label}: every filtered bin_sim pair really carries the tag",
            all(
                FILE_TAG in _tags_of(row, "file_user_tags_a", "file_user_tags_b")
                for row in rows
            ),
            f"first pair a={rows[0].get('file_user_tags_a')} b={rows[0].get('file_user_tags_b')}",
        )
        excluded = _search_rows(
            "/api/bin_sim/search", dict(ns, exclude_file_tag=FILE_TAG, limit=50), "results"
        )
        tagged = {row.get("_id") for row in rows}
        check(
            f"{label}: exclude_file_tag drops exactly the tagged pairs",
            not (tagged & {row.get("_id") for row in excluded})
            and len(excluded) == len(all_pairs) - len(rows),
            f"all={len(all_pairs)} tagged={len(rows)} excluded={len(excluded)}",
        )

    _check_sorting(f"{label}: bin_sim", "/api/bin_sim/search", ns, "results", "score")


def test_search_filters_and_sorting():
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 3c – Filtering and sorting, collection vs pool", BOLD))
    print(_color(f"{'='*60}", CYAN))

    if not file_md5 or not func_id1:
        print(_color("\n[SKIP] No file/function resolved – filter checks skipped.", YELLOW))
        return

    file_entity_id = f"{COLLECTION}:file:{file_md5}"
    pool_created = False

    try:
        # ── Build bin_sim for the collection, BEFORE any tag exists ────────
        print(_color("\n  [Builds (no tags yet)]", BOLD))
        if file_md5_2:
            built = test_endpoint(
                "POST",
                "/api/bin_sim/build",
                data={"collection": COLLECTION, "algo": "unweighted_cosine"},
                label="POST /api/bin_sim/build (collection)",
            )
            if isinstance(built, dict) and built.get("job_id"):
                wait_for_pipeline(
                    built["job_id"], banner=" STEP 3c – Wait for collection bin_sim build"
                )
        else:
            print(_color("     [SKIP] one binary only – bin_sim checks skipped.", YELLOW))

        # The full pool pipeline (sim + clustering + bin_sim), unlike step 3b:
        # here the pool's own bin_sim docs are the thing under test.
        pool = test_endpoint(
            "POST",
            "/api/pool",
            data={
                "pool_id": FILTER_POOL_ID,
                "name": "API Test Filter Pool",
                "collections": [COLLECTION],
                "config": {"only_cross_collection": False},
            },
            label="POST /api/pool (filter pool)",
        )
        if not isinstance(pool, dict) or not pool.get("pool_id"):
            check("filter pool created", False, str(pool)[:120])
            return
        pool_created = True
        wait_for_pipeline(pool.get("job_id"), banner=" STEP 3c – Wait for pool build")

        # ── Tag AFTER both builds ─────────────────────────────────────────
        print(_color("\n  [Tags added after the builds]", BOLD))
        test_endpoint(
            "POST",
            "/api/tags/add",
            data={
                "collection": COLLECTION,
                "entity_type": "file",
                "entity_id": file_entity_id,
                "tag": FILE_TAG,
            },
            label=f"POST /api/tags/add (file, '{FILE_TAG}')",
        )
        test_endpoint(
            "POST",
            "/api/tags/add",
            data={
                "collection": COLLECTION,
                "entity_type": "function",
                "entity_id": func_id1,
                "tag": FUNC_TAG,
            },
            label=f"POST /api/tags/add (function, '{FUNC_TAG}')",
        )

        _check_namespace_filters(
            "collection", {"collection": COLLECTION}, bin_sim_expected=bool(file_md5_2)
        )
        # Pools serve bin_sim from an O(N) scan until reindexed; both paths are
        # separate implementations of the same filters, so both are checked.
        _check_namespace_filters(
            "pool (pre-reindex scan)",
            {"pool": FILTER_POOL_ID},
            bin_sim_expected=bool(file_md5_2),
        )

        if file_md5_2:
            reindexed = test_endpoint(
                "POST",
                "/api/bin_sim/reindex",
                data={"pool_id": FILTER_POOL_ID, "algo": "unweighted_cosine"},
                label="POST /api/bin_sim/reindex (pool)",
            )
            if isinstance(reindexed, dict) and reindexed.get("job_id"):
                wait_for_pipeline(
                    reindexed["job_id"], banner=" STEP 3c – Wait for pool bin_sim reindex"
                )
            _check_namespace_filters(
                "pool (index-backed)", {"pool": FILTER_POOL_ID}, bin_sim_expected=True
            )

    finally:
        for entity_type, entity_id, tag in (
            ("file", file_entity_id, FILE_TAG),
            ("function", func_id1, FUNC_TAG),
        ):
            try:
                requests.post(
                    f"{BASE_URL}/api/tags/remove",
                    json={
                        "collection": COLLECTION,
                        "entity_type": entity_type,
                        "entity_id": entity_id,
                        "tag": tag,
                    },
                    timeout=30,
                )
            except Exception:
                pass
        if pool_created:
            try:
                from bsimvis.app.services.pool_service import pool_service

                pool_service.delete_pool(FILTER_POOL_ID)
                print(_color(f"\n  Pool {FILTER_POOL_ID} deleted.", DIM))
            except Exception as exc:
                print(_color(f"\n  Filter pool cleanup failed: {exc}", YELLOW))


# ---------------------------------------------------------------------------
# Step 5 – Print summary
# ---------------------------------------------------------------------------
def print_summary():
    total = len(results)
    passed = sum(1 for r in results if r["success"])
    failed = total - passed

    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" SUMMARY", BOLD))
    print(_color(f"{'='*60}", CYAN))
    print(f"  Total : {total}")
    print(f"  {_color('Passed', GREEN)} : {passed}")
    print(f"  {_color('Failed', RED)} : {failed}")

    if failed:
        print(_color("\n  Failed endpoints:", RED))
        for r in results:
            if not r["success"]:
                print(f"    {_color('✗', RED)} [{r['status']}] {r['label']}")
                if r["body_preview"]:
                    print(f"      {DIM}{r['body_preview']}{RESET}")

    # Save report
    report_path = "api_test_report.json"
    with open(report_path, "w") as fh:
        json.dump(results, fh, indent=2)
    print(f"\n  Report saved → {_color(report_path, CYAN)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(f"  BSimVis API Test Suite", BOLD))
    print(_color(f"  Target: {BASE_URL}", DIM))
    print(_color(f"  Collection: {COLLECTION}", DIM))
    print(_color(f"  Pool: {POOL_ID}", DIM))
    print(_color(f"  Verbose: {VERBOSE}", DIM))
    print(_color(f"{'='*60}", CYAN))

    uploaded = upload_and_start()
    if uploaded:
        wait_for_pipeline()
        test_duplicate_upload()
        upload_second_binary()

    resolve_ids()
    # Before run_all_tests(): that step deletes the collection on its way out.
    test_pool_annotation_propagation()
    test_search_filters_and_sorting()
    run_all_tests()
    print_summary()
