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
# A finished pipeline is only noticed at the next poll, so the interval is dead
# time added to every wait in the suite. One second costs a few more cheap status
# calls and stops the run rounding up to the next multiple of three.
POLL_INTERVAL = 1  # seconds between pipeline status polls
POLL_TIMEOUT = 300  # max seconds to wait for pipeline

VERBOSE = "-v" in sys.argv or "--verbose" in sys.argv


def _argv_opt(name):
    """Value of `--name X` or `--name=X` on the command line, or None."""
    for i, arg in enumerate(sys.argv):
        if arg == name and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
    return None


# Substring of a step's function name; only matching steps run. The upload and
# analysis prelude always runs regardless — every step reads the binaries it
# produces — so this shortens iteration on one area, it does not skip fixtures.
ONLY = _argv_opt("--only")

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
# Ghidra language / compiler-spec listing and upload validation
# ---------------------------------------------------------------------------
def test_ghidra_languages():
    """Checks /api/index/languages and the processor/cspec validation on upload."""
    body = test_endpoint("GET", "/api/index/languages")
    langs = (body or {}).get("languages") or []

    if not check(
        "languages endpoint returns entries",
        langs,
        "empty list -- is GHIDRA_INSTALL_DIR set for the API process?",
    ):
        return

    check(
        "languages carry id + per-language compilers",
        all(l.get("id") and isinstance(l.get("compilers"), list) for l in langs),
    )
    check(
        "x86:LE:64:default is listed",
        any(l["id"] == "x86:LE:64:default" for l in langs),
    )

    x86 = next((l for l in langs if l["id"] == "x86:LE:64:default"), None)
    if x86:
        check(
            "x86:LE:64:default offers a gcc compiler spec",
            any(c["id"] == "gcc" for c in x86["compilers"]),
            f"got: {[c['id'] for c in x86['compilers']]}",
        )

    # Upload validation: bad processor, and a cspec valid for another language
    # but not this one, must both be rejected before the job is queued.
    bad_proc = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            "collection": COLLECTION,
            "file_name": "lang_validation.bin",
            "processor": "nonexistent:LE:64:default",
        },
        raw_body=b"\x7fELF not-a-real-binary",
        expected_ok=False,
        label="POST /api/file/upload?processor=<invalid> (expect 400)",
    )
    check(
        "invalid processor rejected with an error",
        isinstance(bad_proc, dict) and "error" in bad_proc,
        str(bad_proc)[:200],
    )

    bad_cspec = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            "collection": COLLECTION,
            "file_name": "lang_validation.bin",
            "processor": "x86:LE:64:default",
            "cspec": "not_a_cspec",
        },
        raw_body=b"\x7fELF not-a-real-binary",
        expected_ok=False,
        label="POST /api/file/upload?cspec=<invalid> (expect 400)",
    )
    check(
        "invalid cspec for the language rejected",
        isinstance(bad_cspec, dict) and "error" in bad_cspec,
        str(bad_cspec)[:200],
    )


# ---------------------------------------------------------------------------
# Archive uploads: a zip/tar is unpacked and every member analyzed
# ---------------------------------------------------------------------------
def test_archive_upload():
    """Uploads a zip of two binaries and checks both members get a pipeline."""
    import hashlib
    import io
    import zipfile

    coll = f"{COLLECTION}_archive"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("dir/", "")
        zf.writestr("one.bin", b"\x7fELF archive member one")
        zf.writestr("two.bin", b"\x7fELF archive member two")

    # enqueue=false so the pipelines are only registered, never handed to a
    # worker: these members are not real binaries.
    body = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            "collection": coll,
            "file_name": "samples.zip",
            "skip_sim": "true",
            "enqueue": "false",
        },
        raw_body=buf.getvalue(),
        label="POST /api/file/upload (zip with 2 members)",
    )

    check(
        "archive upload queues one pipeline per member",
        isinstance(body, dict) and body.get("file_count") == 2,
        str(body)[:200],
    )
    check(
        "archive upload returns every pipeline id",
        isinstance(body, dict) and len(body.get("pipeline_ids") or []) == 2,
        str(body)[:200],
    )
    check(
        "archive members keep their own names",
        isinstance(body, dict)
        and sorted(f.get("file_name") for f in body.get("files") or [])
        == ["one.bin", "two.bin"],
        str(body)[:200],
    )

    # `upload --metadata` matches its CSV row against the md5 of the *upload*,
    # so on a member that row is inherited, not matched: its facts still apply,
    # its `file_name` does not -- ghidra_job would otherwise store every member
    # under the container's name and they become indistinguishable.
    meta_body = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            "collection": f"{coll}_meta",
            "file_name": "feedname.bin",
            "skip_sim": "true",
            "enqueue": "false",
            "file_metadata_extra": json.dumps(
                {"file_name": "feedname.bin", "yara": ["yara_from_zip"]}
            ),
        },
        raw_body=buf.getvalue(),
        label="POST /api/file/upload (zip + --metadata row)",
    )

    def _job(job_id):
        return requests.get(f"{BASE_URL}/api/jobs/{job_id}", timeout=30).json()

    analyze_payloads = []
    for pid in (meta_body or {}).get("pipeline_ids") or []:
        for tid in (_job(pid) or {}).get("task_ids") or []:
            payload = (_job(tid) or {}).get("payload") or {}
            if "file_metadata_extra" in payload:
                analyze_payloads.append(payload)
    check(
        "inherited metadata does not rename archive members",
        len(analyze_payloads) == 2
        and all("file_name" not in p["file_metadata_extra"] for p in analyze_payloads)
        and sorted(p.get("file_name") for p in analyze_payloads)
        == ["one.bin", "two.bin"],
        str(analyze_payloads)[:300],
    )
    check(
        "inherited metadata still reaches archive members",
        all(
            p["file_metadata_extra"].get("yara") == ["yara_from_zip"]
            for p in analyze_payloads
        ),
        str(analyze_payloads)[:300],
    )

    # Staged rows are matched per binary. A member's md5 only exists after the
    # server unpacks, so the whole map is staged once per batch and each blob
    # looks itself up: an exact match beats the container's inherited row, name
    # included, because it was matched against this file.
    one_md5 = hashlib.md5(b"\x7fELF archive member one").hexdigest()
    stage_batch = str(uuid.uuid4())
    staged = test_endpoint(
        "POST",
        "/api/file/metadata/stage",
        data={
            "batch_uuid": stage_batch,
            "updates": {
                one_md5: {"file_name": "member_one_real.bin", "yara": ["yara_own"]}
            },
        },
        label="POST /api/file/metadata/stage",
    )
    check(
        "staging accepts the batch map",
        isinstance(staged, dict) and staged.get("staged") == 1,
        str(staged)[:200],
    )
    staged_body = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            "collection": f"{coll}_staged",
            "file_name": "feedname.bin",
            "batch_uuid": stage_batch,
            "skip_sim": "true",
            "enqueue": "false",
            "file_metadata_extra": json.dumps(
                {"file_name": "feedname.bin", "yara": ["yara_from_zip"]}
            ),
        },
        raw_body=buf.getvalue(),
        label="POST /api/file/upload (zip, member has a staged row)",
    )
    by_md5 = {}
    for pid in (staged_body or {}).get("pipeline_ids") or []:
        for tid in (_job(pid) or {}).get("task_ids") or []:
            payload = (_job(tid) or {}).get("payload") or {}
            if "file_metadata_extra" in payload:
                by_md5[payload.get("file_md5")] = payload
    own = by_md5.get(one_md5) or {}
    other = next((p for m, p in by_md5.items() if m != one_md5), {})
    check(
        "a member's own staged row wins over the container's",
        (own.get("file_metadata_extra") or {}).get("yara") == ["yara_own"]
        and (own.get("file_metadata_extra") or {}).get("file_name")
        == "member_one_real.bin"
        and own.get("file_name") == "member_one_real.bin",
        str(own)[:300],
    )
    check(
        "a member with no staged row still inherits the container's",
        (other.get("file_metadata_extra") or {}).get("yara") == ["yara_from_zip"]
        and other.get("file_name") == "two.bin",
        str(other)[:300],
    )

    # A .gpr.zip is a Ghidra project, so it must stay a single file. Distinct
    # bytes from the archive above: that one is now a container holding this
    # collection's copy of that md5, and a collection holds one doc per md5.
    gpr_buf = io.BytesIO()
    with zipfile.ZipFile(gpr_buf, "w") as zf:
        zf.writestr("project.prp", b"ghidra project properties")
        zf.writestr("project.rep/idata/~index.bnd", b"\x7fELF project program")
    gpr = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            "collection": coll,
            "file_name": "project.gpr.zip",
            "skip_sim": "true",
            "enqueue": "false",
        },
        raw_body=gpr_buf.getvalue(),
        label="POST /api/file/upload (.gpr.zip stays one file)",
    )
    check(
        ".gpr.zip is not unpacked",
        isinstance(gpr, dict) and "file_count" not in gpr and gpr.get("file_md5"),
        str(gpr)[:200],
    )

    # A wrong password must fail the upload rather than queue garbage. Needs the
    # `zip` CLI, since stdlib zipfile cannot write encrypted archives.
    import shutil
    import subprocess
    import tempfile

    if shutil.which("zip"):
        with tempfile.TemporaryDirectory() as td:
            member = os.path.join(td, "enc.bin")
            with open(member, "wb") as fh:
                fh.write(b"\x7fELF encrypted member")
            zip_path = os.path.join(td, "enc.zip")
            subprocess.run(
                ["zip", "-P", "infected", "-j", "-q", zip_path, member], check=True
            )
            enc_bytes = open(zip_path, "rb").read()

        bad = test_endpoint(
            "POST",
            "/api/file/upload",
            params={
                "collection": coll,
                "file_name": "enc.zip",
                "archive_password": "wrong",
                "skip_sim": "true",
                "enqueue": "false",
            },
            raw_body=enc_bytes,
            expected_ok=False,
            label="POST /api/file/upload (wrong archive password, expect 400)",
        )
        check(
            "wrong archive password rejected",
            isinstance(bad, dict) and "error" in bad,
            str(bad)[:200],
        )

        ok = test_endpoint(
            "POST",
            "/api/file/upload",
            params={
                "collection": coll,
                "file_name": "enc.zip",
                "skip_sim": "true",
                "enqueue": "false",
            },
            raw_body=enc_bytes,
            label="POST /api/file/upload (encrypted zip, default password)",
        )
        check(
            "encrypted zip unpacks with the default 'infected' password",
            isinstance(ok, dict) and ok.get("file_count") == 1,
            str(ok)[:200],
        )
    else:
        print(_color("[SKIP] `zip` CLI missing – password checks skipped.", YELLOW))

    for name in (coll, f"{coll}_meta", f"{coll}_staged"):
        requests.post(
            f"{BASE_URL}/api/collection/delete", json={"collection": name}, timeout=60
        )


def test_unpack_upload():
    """Checks the pluggable unpack layer: APK, fat Mach-O, UPX and opt-out."""
    import io
    import struct
    import zipfile

    coll = f"{COLLECTION}_unpack"
    # enqueue=false everywhere: these are synthetic blobs, not real binaries, so
    # they must be registered but never handed to a worker.
    common = {"collection": coll, "skip_sim": "true", "enqueue": "false"}

    # -- APK: resources dropped, dex and native libraries kept ---------------
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("AndroidManifest.xml", b"manifest")
        zf.writestr("classes.dex", b"dex\n035\x00payload")
        zf.writestr("lib/arm64-v8a/libfoo.so", b"\x7fELF native lib")
        zf.writestr("res/drawable/icon.png", b"\x89PNG not code")
    apk = test_endpoint(
        "POST",
        "/api/file/upload",
        params={**common, "file_name": "app.apk"},
        raw_body=buf.getvalue(),
        label="POST /api/file/upload (apk keeps only code members)",
    )
    check(
        "apk ingests dex and native libs, not resources",
        isinstance(apk, dict)
        and sorted(f.get("file_name") for f in apk.get("files") or [])
        == ["classes.dex", "lib/arm64-v8a/libfoo.so"],
        str(apk)[:200],
    )

    # -- fat Mach-O: one child per architecture slice ------------------------
    slices = [
        (0x01000007, b"\xcf\xfa\xed\xfe x86_64 slice"),
        (0x0100000C, b"\xcf\xfa\xed\xfe arm64 slice"),
    ]
    header = struct.pack(">II", 0xCAFEBABE, len(slices))
    body = b""
    for cputype, payload in slices:
        header += struct.pack(
            ">IIIII", cputype, 0, 8 + 20 * len(slices) + len(body), len(payload), 4
        )
        body += payload
    fat = test_endpoint(
        "POST",
        "/api/file/upload",
        params={**common, "file_name": "tool"},
        raw_body=header + body,
        label="POST /api/file/upload (fat Mach-O splits per arch)",
    )
    check(
        "fat Mach-O yields one file per architecture",
        isinstance(fat, dict)
        and sorted(f.get("file_name") for f in fat.get("files") or [])
        == ["tool:arm64", "tool:x86_64"],
        str(fat)[:200],
    )

    # -- unpack=false opts out entirely --------------------------------------
    # A different APK from the one above: that upload's container now owns its
    # md5, and re-sending the same bytes is a duplicate whatever unpack says.
    flat_buf = io.BytesIO()
    with zipfile.ZipFile(flat_buf, "w") as zf:
        zf.writestr("AndroidManifest.xml", b"manifest of the opt-out apk")
        zf.writestr("classes.dex", b"dex\n035\x00opt-out payload")
    flat = test_endpoint(
        "POST",
        "/api/file/upload",
        params={**common, "file_name": "app2.apk", "unpack": "false"},
        raw_body=flat_buf.getvalue(),
        label="POST /api/file/upload?unpack=false (stays one file)",
    )
    check(
        "unpack=false analyzes the upload as-is",
        isinstance(flat, dict) and "file_count" not in flat and flat.get("file_md5"),
        str(flat)[:200],
    )

    # -- a declared parent is honoured on a plain binary ---------------------
    declared = test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            **common,
            "file_name": "hand_unpacked.bin",
            "parent_md5": "0" * 32,
            "parent_file_name": "outer.7z",
        },
        raw_body=b"\x7fELF unpacked out of band",
        label="POST /api/file/upload?parent_md5=... (declared parent)",
    )
    check(
        "declared parent keeps the flat single-file response",
        isinstance(declared, dict) and declared.get("file_name") == "hand_unpacked.bin",
        str(declared)[:200],
    )

    # -- UPX: packed and unpacked both analyzed ------------------------------
    import shutil
    import subprocess
    import tempfile

    from bsimvis.app.services import unpack_service

    upx = unpack_service.upx_path()
    if upx and os.path.isfile(TEST_BINARY):
        with tempfile.TemporaryDirectory() as td:
            packed_path = os.path.join(td, "packed.bin")
            shutil.copy(TEST_BINARY, packed_path)
            proc = subprocess.run([upx, "-q", "-f", packed_path], capture_output=True)
            packed = open(packed_path, "rb").read() if proc.returncode == 0 else None

        if packed is None:
            print(_color("[SKIP] upx could not pack the test binary.", YELLOW))
        else:
            res = test_endpoint(
                "POST",
                "/api/file/upload",
                params={**common, "file_name": "packed.bin"},
                raw_body=packed,
                label="POST /api/file/upload (UPX-packed binary)",
            )
            names = sorted(f.get("file_name") for f in (res or {}).get("files") or [])
            check(
                "UPX upload analyzes the packed binary and its unpacked child",
                isinstance(res, dict)
                and res.get("file_count") == 2
                and names == ["packed.bin", "packed.bin.unpacked"],
                str(res)[:200],
            )
    else:
        print(_color("[SKIP] upx not installed – UPX upload check skipped.", YELLOW))

    requests.post(
        f"{BASE_URL}/api/collection/delete", json={"collection": coll}, timeout=60
    )


def test_lineage():
    """Checks containment lineage: container docs, edges, multi-parent, dangling."""
    import hashlib
    import io
    import zipfile

    coll = f"{COLLECTION}_lineage"
    # enqueue=false: children never reach a worker, so only the container gets a
    # document here. That is exactly the case the `exists` flag exists for.
    common = {"collection": coll, "skip_sim": "true", "enqueue": "false"}

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("AndroidManifest.xml", b"manifest")
        zf.writestr("classes.dex", b"dex\n035\x00lineage payload")
        zf.writestr("lib/arm64-v8a/libfoo.so", b"\x7fELF lineage native lib")
    apk_bytes = buf.getvalue()
    apk_md5 = hashlib.md5(apk_bytes).hexdigest()
    dex_md5 = hashlib.md5(b"dex\n035\x00lineage payload").hexdigest()

    test_endpoint(
        "POST",
        "/api/file/upload",
        params={**common, "file_name": "app.apk"},
        raw_body=apk_bytes,
        label="POST /api/file/upload (apk, for lineage)",
    )

    down = test_endpoint(
        "GET",
        f"/api/file/{apk_md5}/lineage",
        params={"collection": coll},
        label="GET /api/file/<md5>/lineage (container)",
    )
    check(
        "container gets an identity document of its own",
        isinstance(down, dict)
        and down.get("file", {}).get("exists") is True
        and down["file"].get("is_container") is True
        and down["file"].get("file_name") == "app.apk",
        str(down)[:250],
    )
    child_paths = sorted(
        c.get("path_in_parent") for c in (down or {}).get("children", [])
    )
    check(
        "container lists its children with their path inside it",
        child_paths == ["classes.dex", "lib/arm64-v8a/libfoo.so"],
        str(child_paths)[:250],
    )
    check(
        "a child with no document yet is reported as not existing",
        all(c.get("exists") is False for c in (down or {}).get("children", [])),
        str(down)[:250],
    )

    # The same containment recorded a second time under a less specific path --
    # what the indexer does, since it only ever knows the file name. It is one
    # edge, so it must stay one child, keeping the path that says the most.
    test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            **common,
            "file_name": "libfoo.so",
            "parent_md5": apk_md5,
        },
        raw_body=b"\x7fELF lineage native lib",
        label="POST /api/file/upload (same child, path-less edge)",
    )
    again = test_endpoint(
        "GET",
        f"/api/file/{apk_md5}/lineage",
        params={"collection": coll},
        label="GET /api/file/<md5>/lineage (after a duplicate edge)",
    )
    dup_paths = sorted(
        c.get("path_in_parent") for c in (again or {}).get("children", [])
    )
    check(
        "an edge recorded twice stays one child, on its fullest path",
        dup_paths == ["classes.dex", "lib/arm64-v8a/libfoo.so"]
        and (again or {}).get("child_count") == 2,
        str(dup_paths)[:250],
    )

    up = test_endpoint(
        "GET",
        f"/api/file/{dex_md5}/lineage",
        params={"collection": coll},
        label="GET /api/file/<md5>/lineage (child)",
    )
    check(
        "child resolves back to its container",
        isinstance(up, dict)
        and [p.get("file_md5") for p in up.get("parents", [])] == [apk_md5]
        and up["parents"][0].get("file_name") == "app.apk"
        and up["parents"][0].get("exists") is True,
        str(up)[:250],
    )
    check(
        "ancestors are ordered nearest first",
        isinstance(up, dict)
        and [a.get("file_md5") for a in up.get("ancestors", [])] == [apk_md5],
        str(up)[:250],
    )
    # A lineage node is rendered by the same row renderer as a search hit, so
    # it has to carry the same document, not a hand-picked handful of fields.
    lineage_only = {
        "file_md5",
        "file_id",
        "collection",
        "path_in_parent",
        "file_name",
        "exists",
        "is_container",
        "child_count",
        "function_count",
        "filetype",
        "tags",
        "user_tags",
    }
    parent = (up or {}).get("parents", [{}])[0]
    check(
        "a lineage node carries the whole file document",
        parent.get("file_id") == f"{coll}:file:{apk_md5}"
        and parent.get("collection") == coll
        and isinstance(parent.get("tags"), list)
        and isinstance(parent.get("user_tags"), list)
        and bool(set(parent) - lineage_only),
        str(parent)[:250],
    )

    # The same member inside a second container: the upload is rejected as a
    # duplicate md5, but that container holding it is still new information.
    buf2 = io.BytesIO()
    with zipfile.ZipFile(buf2, "w") as zf:
        zf.writestr("AndroidManifest.xml", b"manifest two")
        zf.writestr("classes.dex", b"dex\n035\x00lineage payload")
    apk2_md5 = hashlib.md5(buf2.getvalue()).hexdigest()
    test_endpoint(
        "POST",
        "/api/file/upload",
        params={**common, "file_name": "app2.apk"},
        raw_body=buf2.getvalue(),
        # Its only member already exists, so the upload itself reports 400.
        expected_ok=False,
        label="POST /api/file/upload (second apk sharing a member)",
    )
    multi = test_endpoint(
        "GET",
        f"/api/file/{dex_md5}/lineage",
        params={"collection": coll},
        label="GET /api/file/<md5>/lineage (multi-parent)",
    )
    check(
        "a member shared by two containers keeps both parents",
        isinstance(multi, dict)
        and sorted(p.get("file_md5") for p in multi.get("parents", []))
        == sorted([apk_md5, apk2_md5]),
        str(multi)[:250],
    )

    # A declared parent we were never given: the edge stands, the node does not.
    test_endpoint(
        "POST",
        "/api/file/upload",
        params={
            **common,
            "file_name": "hand_unpacked.bin",
            "parent_md5": "0" * 32,
            "parent_file_name": "outer.7z",
            "path_in_parent": "bin/hand_unpacked.bin",
        },
        raw_body=b"\x7fELF lineage out of band",
        label="POST /api/file/upload (declared parent, for lineage)",
    )
    declared_md5 = hashlib.md5(b"\x7fELF lineage out of band").hexdigest()
    dangling = test_endpoint(
        "GET",
        f"/api/file/{declared_md5}/lineage",
        params={"collection": coll},
        label="GET /api/file/<md5>/lineage (declared parent)",
    )
    parent0 = (
        (dangling or {}).get("parents", [{}])[0]
        if (dangling or {}).get("parents")
        else {}
    )
    check(
        "a declared container that was never uploaded is flagged, not hidden",
        parent0.get("file_md5") == "0" * 32
        and parent0.get("exists") is False
        and parent0.get("path_in_parent") == "bin/hand_unpacked.bin",
        str(dangling)[:250],
    )

    # Containers are ordinary rows in the file list.
    listing = test_endpoint(
        "GET",
        "/api/file/search",
        params={"collection": coll, "file_name": "app.apk"},
        label="GET /api/file/search (container is listed)",
    )
    check(
        "container is visible in the file list",
        isinstance(listing, dict)
        and any(f.get("file_md5") == apk_md5 for f in (listing.get("files") or [])),
        str(listing)[:250],
    )

    # The file list draws its expand toggle from child_count, so a row has to
    # say whether it opens without the UI guessing from tags.
    apk_row = next(
        (f for f in (listing or {}).get("files") or [] if f.get("file_md5") == apk_md5),
        {},
    )
    check(
        "a container row carries the child count that drives the tree toggle",
        apk_row.get("child_count") == 2 and apk_row.get("is_container") is True,
        str(apk_row)[:250],
    )

    details = test_endpoint(
        "GET",
        f"/api/file/details/{apk_md5}",
        params={"collection": coll},
        label="GET /api/file/details/<md5> (container)",
    )
    check(
        "the file view is told the file is a container and how much it holds",
        isinstance(details, dict)
        and details.get("file", {}).get("is_container") is True
        and details["file"].get("child_count") == 2,
        str(details)[:250],
    )

    # Nested tree rows need the same signal per lineage node: a node that holds
    # nothing gets no toggle of its own.
    check(
        "a lineage node reports its own child count for nested expansion",
        all(c.get("child_count") == 0 for c in (down or {}).get("children", [])),
        str((down or {}).get("children"))[:250],
    )

    requests.post(
        f"{BASE_URL}/api/collection/delete", json={"collection": coll}, timeout=60
    )


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

        # Per-job pause: hold this pipeline back, confirm the flag is readable,
        # then always resume so the suite leaves nothing stuck.
        paused = test_endpoint(
            "POST", f"/api/jobs/{pipeline_id}/pause", label="POST /api/jobs/<id>/pause"
        )
        check("pause reports paused", (paused or {}).get("paused") is True, str(paused))
        after = test_endpoint(
            "GET", f"/api/jobs/{pipeline_id}", label="GET /api/jobs/<id> (paused)"
        )
        check(
            "paused flag visible on job",
            str((after or {}).get("paused", "")) == "1",
            str((after or {}).get("paused")),
        )
        resumed = test_endpoint(
            "DELETE",
            f"/api/jobs/{pipeline_id}/pause",
            label="DELETE /api/jobs/<id>/pause",
        )
        check(
            "resume clears paused", (resumed or {}).get("paused") is False, str(resumed)
        )

    test_endpoint(
        "POST",
        "/api/jobs/does-not-exist/pause",
        expected_ok=False,
        label="POST /api/jobs/<missing>/pause -> 404",
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
    bs_search = test_endpoint(
        "GET", "/api/bin_sim/search", params={"collection": COLLECTION}
    )
    if bs_search and bs_search.get("results"):
        first_pair = bs_search["results"][0]
        check(
            "Binary similarity doc contains tags_summary list",
            "tags_summary" in first_pair
            and isinstance(first_pair["tags_summary"], list),
            "Checking for tags_summary list in the diff document",
        )
        if first_pair.get("tags_summary"):
            tag_elem = first_pair["tags_summary"][0]
            check(
                "tags_summary element contains fractional split fields",
                all(
                    k in tag_elem
                    for k in (
                        "tag_id",
                        "score",
                        "contribution_pct",
                        "coverage_pct_a",
                        "coverage_pct_b",
                        "bins",
                    )
                ),
                "Fields: tag_id, score, contribution_pct, coverage_pct_a/b, bins",
            )

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
            params={
                "collection": COLLECTION,
                "file_id": f"{COLLECTION}:file:{file_md5}",
            },
            label="GET /api/notes/file/list?file_id=<id>",
        )

    # ── Misc (index config / details) ──────────────────────────────────────
    print(_color("\n  [Misc]", BOLD))
    test_endpoint("GET", "/api/index/config")
    test_ghidra_languages()
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
            print(
                _color("     [SKIP] no func_id resolved – note checks skipped.", YELLOW)
            )

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


def _search(path, params, key):
    """Returns (rows, total) from a search endpoint, or ([], 0) on any failure."""
    try:
        resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=60)
        if resp.status_code != 200:
            vprint(f"     {path} -> HTTP {resp.status_code}")
            return [], 0
        body = resp.json()
        if not isinstance(body, dict):
            return (body, len(body)) if isinstance(body, list) else ([], 0)
        rows = body.get(key, [])
        if not isinstance(rows, list):
            return [], 0
        return rows, int(body.get("total", len(rows)) or len(rows))
    except Exception as exc:
        vprint(f"     {path} error: {exc}")
        return [], 0


def _search_rows(path, params, key):
    return _search(path, params, key)[0]


def _tags_of(row, *fields):
    out = set()
    for f in fields:
        v = row.get(f) or []
        out.update(t.lower() for t in v if isinstance(t, str))
    return out


# ---------------------------------------------------------------------------
# Generic filter/sort sweep
#
# One spec per searchable endpoint instead of a hand-written check each: the
# routes accept ~130 params between them and the interesting failures are all
# the same shapes — a sort that isn't ordered, a sort that changes which rows
# come back, a filter that doesn't narrow, a filter whose rows don't satisfy it.
#
# "sorts" maps each sort_by value to the row field carrying it, which is not
# always the same name: bin_sim sorts "coverage" via coverage_a, cluster sorts
# "count" via member_count but exposes it as "count". Where a sort orders on
# data the row never exposes, the field is None and only the set-equality half
# runs — a check that cannot see the value must not pretend to verify it.
# ---------------------------------------------------------------------------
SEARCH_SPECS = [
    {
        "name": "files",
        "path": "/api/file/search",
        "key": "files",
        "pool": True,
        "sorts": {
            # file_name is this endpoint's DEFAULT sort, so it is the one that
            # must not silently return arbitrary order.
            "file_name": "file_name",
            "language_id": "language_id",
            "function_count": "function_count",
            "bsim_features_count": "bsim_features_count",
            "cohesion_score": "cohesion_score",
            "entry_date": "entry_date",
        },
        "ranges": [
            ("min_function_count", "function_count", "min"),
            ("max_function_count", "function_count", "max"),
            ("min_bsim_features", "bsim_features_count", "min"),
            ("max_bsim_features", "bsim_features_count", "max"),
            ("min_entry_date", "entry_date", "min"),
            ("max_entry_date", "entry_date", "max"),
        ],
        "substr": [("file_name", "file_name"), ("md5", "file_md5")],
    },
    {
        "name": "functions",
        "path": "/api/function/search",
        "key": "functions",
        "pool": True,
        "sorts": {
            "instruction_count": "instruction_count",
            "bsim_features_count": "bsim_features_count",
        },
        "ranges": [("min_features", "bsim_features_count", "min")],
        "substr": [
            ("file_name", "file_name"),
            ("md5", "file_md5"),
            ("namespace", "namespace"),
            ("ret_type", "return_type"),
        ],
    },
    {
        "name": "similarities",
        "path": "/api/similarity/search",
        "key": "pairs",
        "pool": True,
        "base": {"min_score": 0.0},
        "sorts": {"score": "score", "feat_count": "feat_count", "min_features": None},
        "ranges": [
            ("min_score", "score", "min"),
            ("max_score", "score", "max"),
            ("min_features", "feat_count", "min"),
        ],
        "substr": [],
    },
    {
        "name": "bin_sim",
        "path": "/api/bin_sim/search",
        "key": "results",
        "pool": True,
        "sorts": {
            "score": "score",
            "coverage": "coverage_a",
            "shared_clusters": "shared_clusters",
            "functions_count": "functions_count_a",
            "computed_at": "computed_at",
            "architecture": "architecture_a",
        },
        "ranges": [
            ("min_score", "score", "min"),
            ("max_score", "score", "max"),
            ("min_shared", "shared_clusters", "min"),
            ("max_shared", "shared_clusters", "max"),
        ],
        "substr": [("file_name", "file_name_a"), ("md5", "md5_a")],
    },
    {
        "name": "features",
        "path": "/api/feature/search",
        "key": "features",
        "pool": False,
        "sorts": {"tf_score": "tf_score", "frequency": "frequency"},
        "ranges": [
            ("min_frequency", "frequency", "min"),
            ("max_frequency", "frequency", "max"),
            ("min_tf_score", "tf_score", "min"),
            ("max_tf_score", "tf_score", "max"),
        ],
        "substr": [("type", "type"), ("op", "op")],
    },
    {
        "name": "clusters",
        "path": "/api/cluster/list",
        "key": "results",
        "pool": True,
        "sorts": {
            "count": "count",
            "stability": "avg_stability",
            "features": "avg_features",
            "cohesion": "cohesion_score",
        },
        "ranges": [
            ("min_count", "count", "min"),
            ("max_count", "count", "max"),
            ("min_stability", "avg_stability", "min"),
            ("min_features", "avg_features", "min"),
            ("min_cohesion", "cohesion_score", "min"),
        ],
        "substr": [("cluster_name", "cluster_name")],
    },
    {
        "name": "bin_clusters",
        "path": "/api/bin_cluster/list",
        "key": "results",
        "pool": True,
        "sorts": {
            "count": "count",
            "stability": "avg_stability",
            "cohesion": "cohesion_score",
        },
        "ranges": [
            ("min_count", "count", "min"),
            ("max_count", "count", "max"),
            ("min_cohesion", "cohesion_score", "min"),
        ],
        "substr": [("cluster_name", "cluster_name")],
    },
]

# Pools are global, not scoped to a collection or pool namespace.
POOLS_SPEC = {
    "name": "pools",
    "path": "/api/pool",
    "key": "pools",
    "sorts": {
        "name": "name",
        "id": "id",
        "sync_status": "sync_status",
        "created_at": "created_at",
        "last_built_at": "last_built_at",
        "total_files": "total_files",
        "total_functions": "total_functions",
        "total_func_similarities": "total_func_similarities",
        "total_func_clusters": "total_func_clusters",
        "total_file_similarities": "total_file_similarities",
        "total_file_clusters": "total_file_clusters",
    },
    "ranges": [
        ("min_created_at", "created_at", "min"),
        ("max_created_at", "created_at", "max"),
        ("min_total_files", "total_files", "min"),
    ],
    "substr": [("name", "name"), ("id", "id")],
}


def _sort_key(v):
    """Comparable key across the mixed types rows carry (None/str/number)."""
    if v is None:
        return (0, 0.0, "")
    if isinstance(v, bool):
        return (1, float(v), "")
    if isinstance(v, (int, float)):
        return (1, float(v), "")
    return (2, 0.0, str(v).lower())


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _sweep_sorts(spec, ns, label):
    for field, row_field in spec["sorts"].items():
        base = dict(spec.get("base", {}), **ns)
        desc, total = _search(
            spec["path"],
            dict(base, sort_by=field, sort_order="desc", limit=50),
            spec["key"],
        )
        asc, _ = _search(
            spec["path"],
            dict(base, sort_by=field, sort_order="asc", limit=50),
            spec["key"],
        )
        if len(desc) < 2:
            vprint(f"     [skip] {label} {spec['name']}: <2 rows to sort by {field}")
            continue
        # Set equality only holds on a complete result. Once the total spills past
        # the page, desc and asc legitimately return opposite ends of the data.
        if total <= len(desc):
            ids_d = sorted(_json_ids(desc))
            ids_a = sorted(_json_ids(asc))
            check(
                f"{label}: {spec['name']} sort_by={field} keeps the same result set",
                ids_d == ids_a,
                f"desc={len(ids_d)} asc={len(ids_a)} rows",
            )
        else:
            vprint(
                f"     [skip] {label} {spec['name']}: {total} rows > page, set equality N/A"
            )
        if not row_field:
            vprint(
                f"     [skip] {label} {spec['name']}: {field} not exposed on the row"
            )
            continue
        if any(row_field not in row for row in desc):
            vprint(f"     [skip] {label} {spec['name']}: rows lack '{row_field}'")
            continue
        d_keys = [_sort_key(row.get(row_field)) for row in desc]
        a_keys = [_sort_key(row.get(row_field)) for row in asc]
        check(
            f"{label}: {spec['name']} sort_by={field} desc is ordered",
            all(a >= b for a, b in zip(d_keys, d_keys[1:])),
            f"{[row.get(row_field) for row in desc][:5]}",
        )
        check(
            f"{label}: {spec['name']} sort_by={field} asc is ordered",
            all(a <= b for a, b in zip(a_keys, a_keys[1:])),
            f"{[row.get(row_field) for row in asc][:5]}",
        )


def _json_ids(rows):
    """Stable per-row identity for set comparison across sort orders."""
    out = []
    for row in rows:
        rid = (
            row.get("_id")
            or row.get("function_id")
            or row.get("file_id")
            or row.get("cluster_uuid")
            or row.get("cluster_id")
            or row.get("id")
            or row.get("hash")
        )
        if rid is None:
            rid = json.dumps(row, sort_keys=True)[:200]
        out.append(str(rid))
    return out


def _sweep_ranges(spec, ns, label):
    """Pick the threshold from live data (median), so the filter is exercised
    rather than trivially matching everything or nothing."""
    base = dict(spec.get("base", {}), **ns)
    baseline, total = _search(spec["path"], dict(base, limit=100), spec["key"])
    if len(baseline) < 2:
        vprint(f"     [skip] {label} {spec['name']}: <2 rows for range filters")
        return
    complete = total <= len(baseline)  # baseline saw everything -> exact counts hold
    for param, row_field, kind in spec.get("ranges", []):
        vals = sorted(
            v for v in (_num(row.get(row_field)) for row in baseline) if v is not None
        )
        if len(vals) < 2 or vals[0] == vals[-1]:
            vprint(f"     [skip] {label} {spec['name']}: {row_field} has no spread")
            continue
        threshold = vals[len(vals) // 2]
        # Several routes parse these with int(), which raises on "20.0" and makes
        # the filter silently default to off. Send integral values as integers.
        sent = int(threshold) if float(threshold).is_integer() else threshold
        rows, f_total = _search(
            spec["path"], dict(base, limit=100, **{param: sent}), spec["key"]
        )
        got = [v for v in (_num(row.get(row_field)) for row in rows) if v is not None]
        if kind == "min":
            bad = [v for v in got if v < threshold - 1e-6]
            expected = sum(1 for v in vals if v >= threshold)
        else:
            bad = [v for v in got if v > threshold + 1e-6]
            expected = sum(1 for v in vals if v <= threshold)
        check(
            f"{label}: {spec['name']} {param}={sent} returns only matching rows",
            not bad,
            f"{len(got)} row(s), out-of-range={bad[:3]}",
        )
        if complete:
            check(
                f"{label}: {spec['name']} {param}={sent} narrows the result set",
                f_total == expected,
                f"baseline={total} filtered={f_total} expected={expected}",
            )
        else:
            check(
                f"{label}: {spec['name']} {param}={sent} narrows the result set",
                f_total <= total,
                f"baseline={total} filtered={f_total} (partial page, exact count N/A)",
            )


def _sweep_substr(spec, ns, label):
    """Substring filters: take a real value from the data, assert every row carries it."""
    base = dict(spec.get("base", {}), **ns)
    baseline = _search_rows(spec["path"], dict(base, limit=100), spec["key"])
    if not baseline:
        return
    for param, row_field in spec.get("substr", []):
        source = next(
            (str(row[row_field]) for row in baseline if row.get(row_field)), None
        )
        if not source or len(source) < 3:
            vprint(f"     [skip] {label} {spec['name']}: no usable {row_field} value")
            continue
        needle = source[: max(3, len(source) // 2)]
        rows = _search_rows(
            spec["path"], dict(base, limit=100, **{param: needle}), spec["key"]
        )
        check(
            f"{label}: {spec['name']} {param}~'{needle}' returns rows",
            bool(rows),
            f"{len(rows)} of {len(baseline)}",
        )
        if rows:
            # The value can legitimately match a sibling field (md5 vs parent_md5,
            # file_name vs related_file_name), so only require SOME field to carry it.
            hits = sum(
                1
                for row in rows
                if any(
                    needle.lower() in str(v).lower()
                    for v in row.values()
                    if isinstance(v, (str, int, float))
                )
            )
            check(
                f"{label}: {spec['name']} {param}~'{needle}' rows all contain it",
                hits == len(rows),
                f"{hits}/{len(rows)} row(s) carry '{needle}'",
            )


def _sweep_bin_sim_pair_filters(ns, label):
    """bin_sim filters that constrain BOTH sides of a pair, which the generic
    range sweep (one row field at a time) cannot express."""
    base = dict(ns, limit=100)
    baseline, total = _search("/api/bin_sim/search", base, "results")
    if not baseline:
        vprint(f"     [skip] {label} bin_sim: no pairs for pair-level filters")
        return

    def sides(row):
        return _num(row.get("functions_count_a")), _num(row.get("functions_count_b"))

    vals = sorted(
        v for row in baseline for v in sides(row) if v is not None
    )
    if vals and vals[0] != vals[-1]:
        threshold = vals[len(vals) // 2]
        sent = int(threshold) if float(threshold).is_integer() else threshold
        rows, _ = _search("/api/bin_sim/search", dict(base, min_funcs=sent), "results")
        bad = [
            row
            for row in rows
            if any(v is None or v < threshold - 1e-6 for v in sides(row))
        ]
        check(
            f"{label}: bin_sim min_funcs={sent} constrains both sides of the pair",
            not bad,
            f"{len(rows)} row(s), {len(bad)} with a side under the threshold",
        )
    else:
        vprint(f"     [skip] {label} bin_sim: functions_count has no spread")

    # Container modes partition the pairs: none = pairs with no container side,
    # any = its complement, both is a subset of any.
    totals = {}
    for mode in ("both", "any", "none"):
        _, totals[mode] = _search(
            "/api/bin_sim/search", dict(base, containers=mode), "results"
        )
    check(
        f"{label}: bin_sim containers=none/any partition every pair",
        totals["none"] + totals["any"] == total,
        f"none={totals['none']} any={totals['any']} all={total}",
    )
    check(
        f"{label}: bin_sim containers=both is a subset of containers=any",
        totals["both"] <= totals["any"],
        f"both={totals['both']} any={totals['any']}",
    )


def _sweep_namespace(label, ns):
    print(_color(f"\n  [{label}: filter/sort sweep]", BOLD))
    for spec in SEARCH_SPECS:
        if ns and not spec.get("pool", True) and "pool" in ns:
            continue
        _sweep_sorts(spec, ns, label)
        _sweep_ranges(spec, ns, label)
        _sweep_substr(spec, ns, label)
    _sweep_bin_sim_pair_filters(ns, label)


def _check_namespace_filters(label, ns, bin_sim_expected=True):
    """Runs the filter/sort checks for one namespace ({"collection":..}/{"pool":..})."""
    print(_color(f"\n  [{label}]", BOLD))

    # ── Function level: file tag filters AND is enriched onto the row ──────
    rows = _search_rows(
        "/api/function/search", dict(ns, file_user_tag=FILE_TAG, limit=50), "functions"
    )
    check(
        f"{label}: function search filters by file_user_tag",
        bool(rows),
        f"{len(rows)} row(s)",
    )
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
    check(
        f"{label}: function search filters by func user_tag",
        bool(rows),
        f"{len(rows)} row(s)",
    )
    if rows:
        check(
            f"{label}: function tagged on a tagged file also shows its file tags",
            all(FILE_TAG in _tags_of(row, "file_user_tags") for row in rows),
            f"first row file_user_tags={rows[0].get('file_user_tags')}",
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

    # ── bin_sim level: denormalized copy, tagged after the build ───────────
    if not bin_sim_expected:
        return
    all_pairs = _search_rows("/api/bin_sim/search", dict(ns, limit=50), "results")
    if not all_pairs:
        vprint(f"     [skip] {label}: no bin_sim pairs built")
        return

    rows = _search_rows(
        "/api/bin_sim/search", dict(ns, file_tag=FILE_TAG, limit=50), "results"
    )
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
            "/api/bin_sim/search",
            dict(ns, exclude_file_tag=FILE_TAG, limit=50),
            "results",
        )
        tagged = {row.get("_id") for row in rows}
        check(
            f"{label}: exclude_file_tag drops exactly the tagged pairs",
            not (tagged & {row.get("_id") for row in excluded})
            and len(excluded) == len(all_pairs) - len(rows),
            f"all={len(all_pairs)} tagged={len(rows)} excluded={len(excluded)}",
        )


def test_search_filters_and_sorting():
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 3c – Filtering and sorting, collection vs pool", BOLD))
    print(_color(f"{'='*60}", CYAN))

    if not file_md5 or not func_id1:
        print(
            _color(
                "\n[SKIP] No file/function resolved – filter checks skipped.", YELLOW
            )
        )
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
                    built["job_id"],
                    banner=" STEP 3c – Wait for collection bin_sim build",
                )
        else:
            print(
                _color("     [SKIP] one binary only – bin_sim checks skipped.", YELLOW)
            )

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

        # ── Broad filter/sort sweep across every searchable endpoint ───────
        _sweep_namespace("collection", {"collection": COLLECTION})
        _sweep_namespace("pool", {"pool": FILTER_POOL_ID})
        # Pools are global — swept once, not per namespace.
        print(_color("\n  [pools: filter/sort sweep]", BOLD))
        _sweep_sorts(POOLS_SPEC, {}, "global")
        _sweep_ranges(POOLS_SPEC, {}, "global")
        _sweep_substr(POOLS_SPEC, {}, "global")

        if file_md5_2:
            reindexed = test_endpoint(
                "POST",
                "/api/bin_sim/reindex",
                data={"pool_id": FILTER_POOL_ID, "algo": "unweighted_cosine"},
                label="POST /api/bin_sim/reindex (pool)",
            )
            if isinstance(reindexed, dict) and reindexed.get("job_id"):
                wait_for_pipeline(
                    reindexed["job_id"],
                    banner=" STEP 3c – Wait for pool bin_sim reindex",
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
# Step 3c-bis – /api/bin_sim/diff paging is cacheable
#
# get_bin_sim() keeps the hydrated diff doc in a process-local cache, because
# hydrating it costs ~2 kvrocks round trips per function and the UI re-requests
# the same pair on every filter/sort change. Two properties have to hold:
#   - a repeated request returns the IDENTICAL page, so nothing downstream
#     (paging, sorting) mutates the cached doc;
#   - swapping md5_a/md5_b returns the swapped orientation, even though the
#     first orientation is already cached — the doc is re-oriented in place by
#     _flip_diff_sides, so a shared cache entry would serve the wrong side.
# Runs after step 3c, which is what builds the collection's bin_sim docs.
# ---------------------------------------------------------------------------
# "all" is the union view the UI actually opens on, and the only one whose rows
# get a per-row `sid` written into them — the write that must not reach the cache.
DIFF_TABLES = ("all", "matched", "unique_to_a", "unique_to_b")


def _diff_page(md5_a, md5_b, table):
    """One page of /api/bin_sim/diff, or None if the request failed."""
    try:
        resp = requests.get(
            f"{BASE_URL}/api/bin_sim/diff",
            params={
                "collection": COLLECTION,
                "md5_a": md5_a,
                "md5_b": md5_b,
                "table": table,
                "sort_col": "similarity",
                "limit": 50,
            },
            timeout=60,
        )
        if resp.status_code != 200:
            vprint(f"     diff {table} -> HTTP {resp.status_code}")
            return None
        return resp.json()
    except Exception as exc:
        vprint(f"     diff {table} error: {exc}")
        return None


def _meta_md5(meta):
    """md5 out of a file_metadata_* block, whatever it calls the field."""
    if not isinstance(meta, dict):
        return None
    return meta.get("file_md5") or meta.get("md5") or meta.get("md5_a")


def test_bin_sim_diff_cache():
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 3c-bis – bin_sim diff caching / orientation", BOLD))
    print(_color(f"{'='*60}", CYAN))

    if not file_md5 or not file_md5_2:
        print(_color("\n[SKIP] Need two binaries – diff cache checks skipped.", YELLOW))
        return

    first = {t: _diff_page(file_md5, file_md5_2, t) for t in DIFF_TABLES}
    if not check(
        "diff cache: pair is served",
        any(p is not None for p in first.values()),
        "no /api/bin_sim/diff page returned for the uploaded pair",
    ):
        return

    # A vacuous comparison of two empty pages would pass no matter what.
    check(
        "diff cache: pair has rows to compare",
        any((p or {}).get("total") for p in first.values()),
        ", ".join(f"{t}={(first[t] or {}).get('total')}" for t in DIFF_TABLES),
    )

    second = {t: _diff_page(file_md5, file_md5_2, t) for t in DIFF_TABLES}
    differing = [
        t
        for t in DIFF_TABLES
        if first[t] is not None
        and (
            second[t] is None
            or first[t].get("total") != second[t].get("total")
            or first[t].get("items") != second[t].get("items")
        )
    ]
    check(
        "diff cache: second request returns an identical page",
        not differing,
        f"differing tables: {differing}",
    )

    # Now the swapped orientation, with the first one already cached.
    flipped = _diff_page(file_md5_2, file_md5, "matched")
    check(
        "diff cache: swapped md5_a/md5_b returns side A = requested md5_a",
        _meta_md5((flipped or {}).get("file_metadata_a")) == file_md5_2,
        f"got {_meta_md5((flipped or {}).get('file_metadata_a'))}, want {file_md5_2}",
    )
    check(
        "diff cache: swapped md5_a/md5_b returns side B = requested md5_b",
        _meta_md5((flipped or {}).get("file_metadata_b")) == file_md5,
        f"got {_meta_md5((flipped or {}).get('file_metadata_b'))}, want {file_md5}",
    )

    # The flipped request must not have re-oriented the cached original.
    again = _diff_page(file_md5, file_md5_2, "matched")
    check(
        "diff cache: original orientation survives a flipped request",
        _meta_md5((again or {}).get("file_metadata_a")) == file_md5
        and (again or {}).get("items") == (first["matched"] or {}).get("items"),
        f"got {_meta_md5((again or {}).get('file_metadata_a'))}, want {file_md5}",
    )

    _check_diff_cache_expiry()


def _check_diff_cache_expiry():
    """Sliding-idle / hard-ceiling expiry, driven directly rather than by sleeping.

    This runs in the test process, so it touches its own import of the module and
    never the cache the live app is using. Timestamps are rewritten in place: a
    real 60s idle wait or a 600s ceiling wait has no business in the suite.
    """
    try:
        from bsimvis.app.routes import bin_sim as bs
    except ImportError as exc:
        print(_color(f"\n[SKIP] bsimvis not importable ({exc}) – expiry checks skipped.", YELLOW))
        return

    def seed(key, age, idle):
        """Put `key` in the cache, hydrated `age`s ago and last read `idle`s ago."""
        doc = {"key": key}
        bs._diff_cache_put((key,), doc)
        now = time.time()
        with bs._DIFF_CACHE_LOCK:
            bs._DIFF_CACHE[(key,)] = (now - age, now - idle, doc)
        return doc

    fresh = seed("ttl-fresh", age=0, idle=0)
    check(
        "diff cache: fresh entry is a hit",
        bs._diff_cache_get(("ttl-fresh",)) is fresh,
        "freshly stored doc was not returned",
    )

    seed("ttl-idle", age=0, idle=bs._DIFF_IDLE_TTL + 1)
    check(
        "diff cache: entry idle past the TTL is dropped",
        bs._diff_cache_get(("ttl-idle",)) is None,
        f"idle > {bs._DIFF_IDLE_TTL}s survived",
    )

    # The point of the sliding window: older than the idle TTL by hydration age,
    # but read recently, so it stays — and the read pushes the idle clock forward.
    slid = seed("ttl-slide", age=bs._DIFF_IDLE_TTL * 2, idle=1)
    hit = bs._diff_cache_get(("ttl-slide",))
    with bs._DIFF_CACHE_LOCK:
        entry = bs._DIFF_CACHE.get(("ttl-slide",))
    check(
        "diff cache: continued use slides the idle window",
        hit is slid and entry is not None and time.time() - entry[1] < 1,
        f"hit={hit is slid}, entry={'missing' if entry is None else round(time.time() - entry[1], 3)}",
    )

    seed("ttl-ceiling", age=bs._DIFF_MAX_AGE + 1, idle=0)
    check(
        "diff cache: entry past the max age is dropped even when in use",
        bs._diff_cache_get(("ttl-ceiling",)) is None,
        f"age > {bs._DIFF_MAX_AGE}s survived a recent read",
    )

    with bs._DIFF_CACHE_LOCK:
        for k in ("ttl-fresh", "ttl-slide"):
            bs._DIFF_CACHE.pop((k,), None)


# ---------------------------------------------------------------------------
# Step 3d – Pool/collection equivalence (absorbed from test_pools.py)
#
# The invariant: how binaries are grouped must not change the analysis. Two
# binaries in ONE collection, built the normal way, must produce exactly what
# the SAME two binaries produce when split across two collections joined by a
# pool. The pool path is a separate implementation end to end — its own sim
# build, its own clustering, its own bin_sim — so this is what stops the two
# from silently diverging.
#
# Compares four things: the bin_sim score, the bin_sim doc (matched/unique
# cluster diff), function similarity (pairs, scores, docs) and function
# clusters (membership + metadata).
#
# All tuning params are omitted everywhere on purpose: both sides then fall
# back to the same config defaults and are compared on equal footing.
# ---------------------------------------------------------------------------
EQ_ARM = "./data/test/v01_arm_x64"
EQ_LINUX = "./data/test/v01_linux_x64"
EQ_ALGO = "unweighted_cosine"


def _clean_fid(fid):
    """{coll}:func:{md5}:{addr} -> {md5}:{addr}. The two sides live in different
    collections, so ids only compare after the prefix is dropped."""
    fid = fid.decode() if isinstance(fid, bytes) else str(fid)
    i = fid.find(":func:")
    return fid[i + 6 :] if i != -1 else fid


def _build_canonical_map(single_pairs, md5_a):
    """Maps interchangeable functions onto one representative.

    Identical functions (same similarity profile) are tie-broken arbitrarily by
    each build, so single and pool can pick different-but-equivalent partners.
    Without this, those ties read as mismatches. Group A functions by their
    profile of (partner, score), then group B functions by their profile of
    (canonical A, score), and elect min() of each group.
    """
    from collections import defaultdict

    profile_a = defaultdict(list)
    for f1, f2, score in single_pairs:
        f1_in_a, f2_in_a = md5_a in f1, md5_a in f2
        if f1_in_a != f2_in_a:  # cross-binary only
            fa, fb = (f1, f2) if f1_in_a else (f2, f1)
            profile_a[fa].append((fb, round(score, 4)))

    groups = defaultdict(list)
    for fa, prof in profile_a.items():
        groups[tuple(sorted(prof))].append(fa)
    canonical = {}
    for _, funcs in groups.items():
        rep = min(funcs)
        for f in funcs:
            canonical[f] = rep

    profile_b = defaultdict(list)
    for f1, f2, score in single_pairs:
        f1_in_a, f2_in_a = md5_a in f1, md5_a in f2
        if f1_in_a != f2_in_a:
            fa, fb = (f1, f2) if f1_in_a else (f2, f1)
            profile_b[fb].append((canonical.get(fa, fa), round(score, 4)))

    groups = defaultdict(list)
    for fb, prof in profile_b.items():
        groups[tuple(sorted(prof))].append(fb)
    for _, funcs in groups.items():
        rep = min(funcs)
        for f in funcs:
            canonical[f] = rep
    return canonical


def _upload_eq(path, collection):
    with open(path, "rb") as fh:
        raw = fh.read()
    resp = requests.post(
        f"{BASE_URL}/api/file/upload",
        params={
            "collection": collection,
            "file_name": os.path.basename(path),
            "batch_name": "Equivalence Run",
            "profile": "fast",
            "min_func_len": 10,
            "skip_sim": "false",
        },
        data=raw,
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def _wait_all(job_ids, what):
    """Polls several jobs to completion. Quiet: one line per batch, not per poll."""
    pending = {j for j in job_ids if j}
    if not pending:
        return
    print(f"     waiting for {what} ({len(pending)} job(s)) …", end="", flush=True)
    deadline = time.time() + POLL_TIMEOUT
    while pending and time.time() < deadline:
        time.sleep(POLL_INTERVAL)
        for j in list(pending):
            try:
                resp = requests.get(f"{BASE_URL}/api/jobs/{j}", timeout=10)
                if resp.status_code == 200:
                    if str(resp.json().get("status", "")).lower() in (
                        "completed",
                        "failed",
                        "cancelled",
                    ):
                        pending.discard(j)
            except Exception:
                pass
    print(" done." if not pending else _color(" TIMEOUT", RED))


def test_tag_vocabulary_and_llm_batch():
    """Tag vocabulary CRUD (/api/tags/list|create|delete|llm) and LLM batch jobs.

    The LLM itself (Ollama) may not be reachable in a test environment, so the
    batch checks cover job acceptance, selection resolution, the size cap and
    cancellation — not the generated content.
    """
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 3d – Tag vocabulary + LLM batch", BOLD))
    print(_color(f"{'='*60}", CYAN))

    if not file_md5 or not func_id1:
        print(
            _color("\n[SKIP] No uploaded file – tag/LLM batch checks skipped.", YELLOW)
        )
        return

    vocab_tag = f"vocab_{uuid.uuid4().hex[:6]}"

    # --- vocabulary entry with no members ---
    test_endpoint(
        "POST",
        "/api/tags/create",
        data={
            "collection": COLLECTION,
            "tag": vocab_tag,
            "color": "#ff00ff",
            "llm": True,
        },
        label="POST /api/tags/create",
    )
    listing = test_endpoint(
        "GET", "/api/tags/list", params={"collection": COLLECTION, "q": vocab_tag}
    )
    row = next(
        (i for i in (listing or {}).get("items", []) if i.get("tag") == vocab_tag), None
    )
    check("tags/list returns the created tag", row is not None, str(row))
    if row:
        check(
            "created tag is flagged for LLM",
            row.get("llm") is True,
            str(row.get("llm")),
        )
        check(
            "created tag has no members",
            row.get("total_count") == 0,
            str(row.get("total_count")),
        )

    # Duplicate creation is refused rather than silently resetting the metadata.
    dup = test_endpoint(
        "POST",
        "/api/tags/create",
        data={"collection": COLLECTION, "tag": vocab_tag},
        expected_ok=False,
        label="POST /api/tags/create (duplicate)",
    )
    check("duplicate tag creation refused", "error" in (dup or {}), str(dup))

    # --- llm flag toggle ---
    test_endpoint(
        "POST",
        "/api/tags/llm",
        data={"collection": COLLECTION, "tag": vocab_tag, "llm": False},
        label="POST /api/tags/llm",
    )
    listing = test_endpoint(
        "GET",
        "/api/tags/list",
        params={"collection": COLLECTION, "q": vocab_tag},
        label="GET /api/tags/list (after llm toggle)",
    )
    row = next(
        (i for i in (listing or {}).get("items", []) if i.get("tag") == vocab_tag), None
    )
    check("llm flag cleared", row is not None and row.get("llm") is False, str(row))

    # --- delete strips the tag from entities, not just the vocabulary ---
    test_endpoint(
        "POST",
        "/api/tags/add",
        data={
            "collection": COLLECTION,
            "entity_type": "function",
            "entity_id": func_id1,
            "tag": vocab_tag,
        },
        label="POST /api/tags/add (before delete)",
    )
    deleted = test_endpoint(
        "POST",
        "/api/tags/delete",
        data={"collection": COLLECTION, "tag": vocab_tag},
        label="POST /api/tags/delete",
    )
    check(
        "delete reports the function it was stripped from",
        (deleted or {}).get("removed", {}).get("function", 0) >= 1,
        str((deleted or {}).get("removed")),
    )
    listing = test_endpoint(
        "GET",
        "/api/tags/list",
        params={"collection": COLLECTION, "q": vocab_tag},
        label="GET /api/tags/list (after delete)",
    )
    check(
        "deleted tag gone from vocabulary",
        not [i for i in (listing or {}).get("items", []) if i.get("tag") == vocab_tag],
        str(listing),
    )
    resp = requests.get(
        f"{BASE_URL}/api/function/search",
        params={"collection": COLLECTION, "user_tag": vocab_tag, "limit": 5},
        timeout=30,
    )
    remaining = resp.json().get("functions", []) if resp.status_code == 200 else []
    check("deleted tag stripped from functions", not remaining, str(len(remaining)))

    # --- batch: explicit ids ---
    started = test_endpoint(
        "POST",
        "/api/llm/batch",
        data={
            "collection": COLLECTION,
            "func_ids": [func_id1],
            "actions": ["notes", "tags"],
        },
        label="POST /api/llm/batch (func_ids)",
    )
    job_id = (started or {}).get("job_id")
    check("batch job created", bool(job_id), str(started))
    check(
        "batch total matches selection", (started or {}).get("total") == 1, str(started)
    )

    if job_id:
        status = test_endpoint("GET", f"/api/llm/batch/{job_id}")
        check(
            "batch status exposes counts and errors",
            isinstance(status, dict) and "counts" in status and "errors" in status,
            str(status)[:200],
        )
        cancelled = test_endpoint(
            "POST",
            f"/api/llm/batch/{job_id}/cancel",
            label="POST /api/llm/batch/<id>/cancel",
        )
        check(
            "batch cancel accepted",
            (cancelled or {}).get("status") == "cancelled",
            str(cancelled),
        )

    # --- batch: filter-based selection resolves server-side ---
    filtered = test_endpoint(
        "POST",
        "/api/llm/batch",
        data={
            "collection": COLLECTION,
            "filters": f"file_md5={file_md5}",
            "actions": ["notes"],
        },
        label="POST /api/llm/batch (filters)",
    )
    check(
        "filter selection resolved to functions",
        (filtered or {}).get("total", 0) > 0,
        str(filtered),
    )
    if (filtered or {}).get("job_id"):
        test_endpoint(
            "POST",
            f"/api/llm/batch/{filtered['job_id']}/cancel",
            label="POST /api/llm/batch/<id>/cancel (filters)",
        )

    # --- batch: size cap refuses oversized selections up front ---
    from bsimvis.app.services.llm_batch_service import max_batch_size

    oversized = test_endpoint(
        "POST",
        "/api/llm/batch",
        data={
            "collection": COLLECTION,
            "func_ids": [
                f"{COLLECTION}:func:{file_md5}:{i:08x}"
                for i in range(max_batch_size() + 1)
            ],
            "actions": ["notes"],
        },
        expected_ok=False,
        label="POST /api/llm/batch (over cap)",
    )
    check("oversized batch refused", "error" in (oversized or {}), str(oversized)[:200])

    # --- invalid action is rejected ---
    bad = test_endpoint(
        "POST",
        "/api/llm/batch",
        data={"collection": COLLECTION, "func_ids": [func_id1], "actions": ["bogus"]},
        expected_ok=False,
        label="POST /api/llm/batch (invalid action)",
    )
    check("invalid action refused", "error" in (bad or {}), str(bad))


def test_pool_collection_equivalence():
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 3d – Pool vs collection equivalence", BOLD))
    print(_color(f"{'='*60}", CYAN))

    for p in (EQ_ARM, EQ_LINUX):
        if not os.path.isfile(p):
            print(_color(f"\n[SKIP] {p} missing – equivalence checks skipped.", YELLOW))
            return

    import json as _json
    from bsimvis.app.services.pool_service import pool_service
    from bsimvis.app.services.redis_client import get_redis

    run = uuid.uuid4().hex[:6]
    single = f"eq_single_{run}"
    sep_arm, sep_linux = f"eq_arm_{run}", f"eq_linux_{run}"
    eq_pool = f"eq_pool_{run}"
    r = get_redis()

    try:
        # ── Ingest: same two binaries, grouped two different ways ─────────
        print(_color("\n  [Ingest]", BOLD))
        jobs, md5_arm, md5_linux = [], None, None
        for path, coll in (
            (EQ_ARM, single),
            (EQ_LINUX, single),
            (EQ_ARM, sep_arm),
            (EQ_LINUX, sep_linux),
        ):
            body = _upload_eq(path, coll)
            jobs.append(body.get("pipeline_id"))
            if path == EQ_ARM:
                md5_arm = body.get("file_md5")
            else:
                md5_linux = body.get("file_md5")
        _wait_all(jobs, "ingestion")
        if not check(
            "equivalence: both binaries ingested", bool(md5_arm and md5_linux)
        ):
            return

        # ── Path A: one collection, the normal build ──────────────────────
        print(_color("\n  [Collection build]", BOLD))
        for path, payload in (
            (
                "/api/similarity/build",
                {"collection": single, "all": True, "algo": EQ_ALGO, "top_k": 1000},
            ),
            ("/api/cluster/build", {"collection": single}),
            ("/api/bin_sim/build", {"collection": single}),
        ):
            resp = requests.post(f"{BASE_URL}{path}", json=payload, timeout=10)
            resp.raise_for_status()
            _wait_all([resp.json().get("job_id")], path.rsplit("/", 2)[-2])

        # ── Path B: two collections joined by a pool ──────────────────────
        print(_color("\n  [Pool build]", BOLD))
        ok, msg = pool_service.create_pool(
            eq_pool,
            "Equivalence Pool",
            [sep_arm, sep_linux],
            {
                "only_cross_collection": False,
                "func_sim_params": {},
                "func_cluster_params": {},
                "file_sim_params": {"enabled": True},
                "file_cluster_params": {"enabled": True},
            },
        )
        if not check("equivalence: pool created over the split collections", ok, msg):
            return
        for path in (f"/api/pool/{eq_pool}/build", f"/api/pool/{eq_pool}/cluster"):
            resp = requests.post(f"{BASE_URL}{path}", timeout=10)
            resp.raise_for_status()
            _wait_all([resp.json().get("job_id")], path.rsplit("/", 1)[-1])

        # ── Canonical map, from the single collection's cross-binary sims ──
        single_scores = r.zrange(
            f"{single}:sim:score:{EQ_ALGO}", 0, -1, withscores=True
        )
        pool_scores = r.zrange(
            f"global:pool:{eq_pool}:sim:score", 0, -1, withscores=True
        )

        single_pairs = []
        for sid_b, score in single_scores:
            sid = sid_b.decode() if isinstance(sid_b, bytes) else str(sid_b)
            parts = sid.split(f":sim:{EQ_ALGO}:")
            if len(parts) == 2:
                ids = parts[1].split("::")
                if len(ids) == 2:
                    single_pairs.append((_clean_fid(ids[0]), _clean_fid(ids[1]), score))
        canonical = _build_canonical_map(single_pairs, md5_arm)

        def canon(fid):
            c = _clean_fid(fid)
            return canonical.get(c, c)

        def parse_sid(sid_b, marker):
            sid = sid_b.decode() if isinstance(sid_b, bytes) else str(sid_b)
            parts = sid.split(marker)
            if len(parts) == 2:
                ids = parts[1].split("::")
                if len(ids) == 2:
                    return tuple(sorted([canon(i) for i in ids]))
            return None

        print(_color("\n  [Equivalence]", BOLD))

        # ── 1. bin_sim score ──────────────────────────────────────────────
        m1, m2 = sorted([md5_arm, md5_linux])
        single_bs_key = f"{single}:bin_sim:{EQ_ALGO}:{m1}::{m2}"
        s_score = r.zscore(f"{single}:bin_sim:score:{EQ_ALGO}", single_bs_key)

        b1, b2 = sorted([(sep_arm, md5_arm), (sep_linux, md5_linux)])
        pool_bs_key = (
            f"global:pool:{eq_pool}:bin_sim:{EQ_ALGO}:{b1[0]}:{b1[1]}::{b2[0]}:{b2[1]}"
        )
        p_score = r.zscore(
            f"global:pool:{eq_pool}:bin_sim:score:{EQ_ALGO}", pool_bs_key
        )

        if s_score is None or p_score is None:
            check(
                "equivalence: bin_sim score exists on both sides",
                False,
                f"single={s_score} pool={p_score}",
            )
        else:
            check(
                "equivalence: bin_sim scores match",
                abs(round(s_score, 3) - round(p_score, 3)) < 1e-5,
                f"single={s_score:.6f} pool={p_score:.6f}",
            )

        # ── 2. bin_sim doc (the cluster diff) ─────────────────────────────
        s_doc = _json.loads(r.get(single_bs_key) or "null")
        p_doc = _json.loads(r.get(pool_bs_key) or "null")

        def norm_diff(diff):
            """Drop namespace-local ids/rarities, canonicalize func ids, sort."""
            if not diff:
                return diff
            out = {}
            for key in (
                "matched",
                "unique_to_a",
                "unique_to_b",
                "unclustered_a",
                "unclustered_b",
            ):
                if key not in diff:
                    continue
                items = []
                for item in diff[key]:
                    norm = {
                        k: v
                        for k, v in item.items()
                        if k
                        not in (
                            "cluster_uuid",
                            "cluster_id",
                            "sim_rarity",
                            "collection_rarity",
                            "avg_features",
                        )
                    }
                    for fk in ("funcs_a", "funcs_b", "funcs"):
                        if norm.get(fk):
                            norm[fk] = sorted(canon(f) for f in norm[fk])
                    for fk in ("func_a", "func_b", "func_id"):
                        if norm.get(fk):
                            norm[fk] = canon(norm[fk])
                    items.append(norm)
                if key == "matched":
                    items.sort(key=lambda x: (x.get("func_a", ""), x.get("func_b", "")))
                elif key in ("unique_to_a", "unique_to_b"):
                    items.sort(key=lambda x: x.get("func_id", ""))
                out[key] = items
            return out

        if not s_doc or not p_doc:
            check(
                "equivalence: bin_sim doc exists on both sides",
                False,
                f"single={bool(s_doc)} pool={bool(p_doc)}",
            )
        else:
            # Pool and collection bin_sim docs use the same field names, so every
            # shared field has to agree — not just the score and the diff.
            floats = (
                "score",
                "coverage_a",
                "coverage_b",
            )
            keep = (
                "md5_a",
                "md5_b",
                "algo",
                "diff",
                "shared_clusters",
                "unique_clusters_a",
                "unique_clusters_b",
            ) + floats
            n_s = {k: v for k, v in s_doc.items() if k in keep}
            n_p = {k: v for k, v in p_doc.items() if k in keep}
            n_s["diff"] = norm_diff(s_doc.get("diff"))
            n_p["diff"] = norm_diff(p_doc.get("diff"))
            for d in (n_s, n_p):
                for f in floats:
                    if d.get(f) is not None:
                        d[f] = round(d[f], 3)
            check(
                "equivalence: bin_sim docs match (matched/unique cluster diff)",
                n_s == n_p,
                (
                    ""
                    if n_s == n_p
                    else f"single={_json.dumps(n_s)[:200]} pool={_json.dumps(n_p)[:200]}"
                ),
            )

        # ── 3. function similarity: pairs, scores, docs ───────────────────
        single_map, single_sids = {}, {}
        for sid_b, score in single_scores:
            k = parse_sid(sid_b, f":sim:{EQ_ALGO}:")
            if k:
                single_map[k] = round(score, 4)
                single_sids[k] = (
                    sid_b.decode() if isinstance(sid_b, bytes) else str(sid_b)
                )
        pool_map, pool_sids = {}, {}
        for sid_b, score in pool_scores:
            k = parse_sid(sid_b, ":sim:")
            if k:
                pool_map[k] = round(score, 4)
                pool_sids[k] = (
                    sid_b.decode() if isinstance(sid_b, bytes) else str(sid_b)
                )

        s_keys, p_keys = set(single_map), set(pool_map)
        check(
            "equivalence: function similarity pairs match",
            s_keys == p_keys,
            f"{len(s_keys)} single / {len(p_keys)} pool; only-single={list(s_keys - p_keys)[:3]} only-pool={list(p_keys - s_keys)[:3]}",
        )
        mismatched = [
            k for k in s_keys & p_keys if abs(single_map[k] - pool_map[k]) > 1e-4
        ]
        check(
            "equivalence: function similarity scores match",
            not mismatched,
            f"{len(mismatched)} mismatch(es), e.g. {mismatched[:2]}",
        )

        doc_mismatch = []
        for k in s_keys & p_keys:
            drop_s = ("collection", "entry_date", "id1", "id2")
            drop_p = drop_s + ("coll_1", "coll_2")
            d_s = _json.loads(r.get(single_sids[k]) or "{}")
            d_p = _json.loads(r.get(pool_sids[k]) or "{}")
            n_s = {a: b for a, b in d_s.items() if a not in drop_s}
            n_p = {a: b for a, b in d_p.items() if a not in drop_p}
            for d in (n_s, n_p):
                if "score" in d:
                    d["score"] = round(d["score"], 4)
            if n_s != n_p:
                doc_mismatch.append(k)
        check(
            "equivalence: detailed function similarity docs match",
            not doc_mismatch,
            f"{len(doc_mismatch)} mismatch(es), e.g. {doc_mismatch[:2]}",
        )

        # ── 4. function clusters: membership + metadata ───────────────────
        def norm_meta(meta):
            out = {
                k: v
                for k, v in meta.items()
                if k
                not in ("collection", "id", "created_at", "cluster_uuid", "cluster_id")
            }
            if "sample_functions" in out:
                samples = [
                    {
                        k: v
                        for k, v in f.items()
                        if k not in ("function_id", "collection")
                    }
                    for f in out["sample_functions"]
                ]
                samples.sort(
                    key=lambda x: (
                        x.get("entrypoint_address", ""),
                        x.get("file_md5", ""),
                    )
                )
                out["sample_functions"] = samples
            return out

        def clusters_of(list_key, member_fmt, meta_fmt):
            out = {}
            for cid_b in r.smembers(list_key):
                cid = cid_b.decode() if isinstance(cid_b, bytes) else str(cid_b)
                members = tuple(
                    sorted(canon(m) for m in r.smembers(member_fmt.format(cid=cid)))
                )
                out[members] = norm_meta(
                    _json.loads(r.get(meta_fmt.format(cid=cid)) or "{}")
                )
            return out

        s_clusters = clusters_of(
            f"{single}:cluster:list:{EQ_ALGO}",
            f"{single}:cluster:{EQ_ALGO}:{{cid}}:members",
            f"{single}:cluster:{EQ_ALGO}:{{cid}}:meta",
        )
        p_clusters = clusters_of(
            f"global:pool:{eq_pool}:cluster:list",
            f"global:pool:{eq_pool}:cluster:{EQ_ALGO}:{{cid}}:members",
            f"global:pool:{eq_pool}:cluster:{EQ_ALGO}:{{cid}}:meta",
        )
        check(
            "equivalence: function cluster membership matches",
            set(s_clusters) == set(p_clusters),
            f"{len(s_clusters)} single / {len(p_clusters)} pool cluster(s)",
        )
        meta_mismatch = [
            k
            for k in set(s_clusters) & set(p_clusters)
            if s_clusters[k] != p_clusters[k]
        ]
        check(
            "equivalence: function cluster metadata matches",
            not meta_mismatch,
            f"{len(meta_mismatch)} mismatch(es)",
        )

    finally:
        # test_pools.py left these behind (its cleanup was commented out), so
        # every run leaked three collections and a pool.
        print(_color("\n  [Cleanup]", DIM))
        del_jobs = []
        for coll in (single, sep_arm, sep_linux):
            try:
                resp = requests.post(
                    f"{BASE_URL}/api/collection/delete",
                    json={"collection": coll},
                    timeout=10,
                )
                if resp.status_code == 200:
                    del_jobs.append(resp.json().get("job_id"))
            except Exception as exc:
                vprint(f"     cleanup of {coll} failed: {exc}")
        _wait_all(del_jobs, "collection cleanup")
        try:
            pool_service.delete_pool(eq_pool)
        except Exception as exc:
            vprint(f"     pool cleanup failed: {exc}")


# ---------------------------------------------------------------------------
# Step 4b2 – Container similarity: child scores roll up the containment edges
#
# An APK holds no code of its own, so build_bin_sim leaves it out of the pair
# sweep. The rollup that runs after it is what lets two containers be compared
# at all, and its whole claim is that the container says exactly what its
# children say, weighted by function count. One container per binary makes that
# claim checkable to the digit: with a single matched child and nothing else in
# either container, the container score must BE the child score.
# ---------------------------------------------------------------------------
def test_container_similarity():
    import hashlib
    import io
    import zipfile

    if not (os.path.isfile(TEST_BINARY) and os.path.isfile(SECOND_BINARY)):
        print(_color("\n[SKIP] container similarity needs both test binaries.", YELLOW))
        return

    coll = f"{COLLECTION}_contsim"
    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 4b2 – Container similarity", BOLD))
    print(_color(f"{'='*60}", CYAN))

    def zip_of(path, inner):
        buf = io.BytesIO()
        with open(path, "rb") as fh:
            payload = fh.read()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(inner, payload)
        return buf.getvalue(), hashlib.md5(payload).hexdigest()

    zip_a, bin_a = zip_of(TEST_BINARY, "lib/one.so")
    zip_b, bin_b = zip_of(SECOND_BINARY, "lib/two.so")
    apk_a = hashlib.md5(zip_a).hexdigest()
    apk_b = hashlib.md5(zip_b).hexdigest()

    pipelines = []
    for name, blob in (("first.apk", zip_a), ("second.apk", zip_b)):
        body = test_endpoint(
            "POST",
            "/api/file/upload",
            params={"collection": coll, "file_name": name},
            raw_body=blob,
            label=f"POST /api/file/upload ({name})",
        )
        pipelines += (body or {}).get("pipeline_ids") or (
            [body.get("pipeline_id")] if isinstance(body, dict) and body.get("pipeline_id") else []
        )

    ok = True
    for pid in pipelines:
        ok = wait_for_pipeline(pid, banner=" STEP 4b2 – Wait for container member analysis") and ok
    if not ok:
        check("container members analysed", False, "pipeline did not complete")
        return

    built = test_endpoint(
        "POST",
        "/api/bin_sim/build",
        data={"collection": coll, "algo": "unweighted_cosine"},
        label="POST /api/bin_sim/build (container collection)",
    )
    if isinstance(built, dict) and built.get("job_id"):
        wait_for_pipeline(built["job_id"], banner=" STEP 4b2 – Wait for bin_sim build")

    def pair_with(md5, other, params=None):
        body = test_endpoint(
            "GET",
            "/api/bin_sim/list",
            params={"collection": coll, "md5": md5, "limit": 50, **(params or {})},
            label=f"GET /api/bin_sim/list (md5={md5[:8]}{' grouped' if params else ''})",
        )
        rows = (body or {}).get("results") or []
        return next(
            (
                row
                for row in rows
                if other in (row.get("md5_a"), row.get("md5_b"))
            ),
            None,
        ), rows

    child, _ = pair_with(bin_a, bin_b)
    check(
        "the two container members are scored against each other",
        isinstance(child, dict) and (child.get("score") or 0) > 0,
        str(child)[:200],
    )
    if not child:
        return

    container, _ = pair_with(apk_a, apk_b)
    check(
        "two containers holding matching code are scored against each other",
        isinstance(container, dict) and container.get("is_container_pair") is True,
        str(container)[:250],
    )
    if not container:
        return

    # The exactness claim. One child on each side and no unmatched mass, so the
    # weighting has nothing to dilute: any drift here means the rollup is
    # weighting, orienting or matching differently than it says it does.
    check(
        "a container with one matched child scores exactly what that child scored",
        abs((container.get("score") or 0) - (child.get("score") or 0)) < 1e-6,
        f"container={container.get('score')} child={child.get('score')}",
    )

    def side(row, md5, field):
        return row.get(f"{field}_a") if row.get("md5_a") == md5 else row.get(f"{field}_b")

    check(
        "container coverage is its child's coverage when the child is all it holds",
        abs((side(container, apk_a, "coverage") or 0) - (side(child, bin_a, "coverage") or 0))
        < 1e-6,
        f"container={side(container, apk_a, 'coverage')} child={side(child, bin_a, 'coverage')}",
    )
    check(
        "the container pair reports how many children it weighed",
        (side(container, apk_a, "child_count") or 0) == 1
        and (side(container, apk_a, "functions_count") or 0) > 0,
        str(container)[:250],
    )

    # Cross-level: the loose binary and the container that holds its twin are
    # the same similarity question asked at two altitudes, so both must answer.
    cross, _ = pair_with(bin_a, apk_b)
    check(
        "a standalone binary is scored against a container holding its match",
        isinstance(cross, dict) and (cross.get("score") or 0) > 0,
        str(cross)[:200],
    )

    # The containers filter reads the live lineage set, so each mode must cut a
    # different slice out of the same pairs: two containers, two loose binaries,
    # and one of each are all present in this collection.
    def searched(mode):
        body = test_endpoint(
            "GET",
            "/api/bin_sim/search",
            params={"collection": coll, "limit": 100, "containers": mode},
            label=f"GET /api/bin_sim/search (containers={mode})",
        )
        return (body or {}).get("results") or []

    def has_pair(rows, x, y):
        return any({row.get("md5_a"), row.get("md5_b")} == {x, y} for row in rows)

    rows = searched("both")
    check(
        "containers=both keeps only container-to-container pairs",
        has_pair(rows, apk_a, apk_b)
        and not has_pair(rows, bin_a, apk_b)
        and not has_pair(rows, bin_a, bin_b),
        f"{len(rows)} row(s)",
    )
    rows = searched("any")
    check(
        "containers=any keeps every pair with at least one container side",
        has_pair(rows, apk_a, apk_b)
        and has_pair(rows, bin_a, apk_b)
        and not has_pair(rows, bin_a, bin_b),
        f"{len(rows)} row(s)",
    )
    rows = searched("none")
    check(
        "containers=none keeps only pairs of plain files",
        has_pair(rows, bin_a, bin_b)
        and not has_pair(rows, apk_a, apk_b)
        and not has_pair(rows, bin_a, apk_b),
        f"{len(rows)} row(s)",
    )

    # Grouping: the match inside second.apk stops being a loose row and becomes
    # evidence hanging off the container's row.
    grouped_row, grouped_rows = pair_with(bin_a, apk_b, {"group": "container"})
    kids = (grouped_row or {}).get("children") or []
    check(
        "grouping folds a match into the container it was extracted from",
        bool(kids) and any(bin_b in (k.get("md5_a"), k.get("md5_b")) for k in kids),
        str(grouped_row)[:300],
    )
    check(
        "a folded match is not also listed loose",
        not any(
            bin_b in (row.get("md5_a"), row.get("md5_b"))
            and not row.get("is_container_pair")
            for row in grouped_rows
        ),
        str([r.get("md5_b") for r in grouped_rows])[:250],
    )

    # The diff view pages child pairs where it pages functions for a normal pair.
    page = test_endpoint(
        "GET",
        "/api/diff",
        params={
            "collection_a": coll,
            "md5_a": apk_a,
            "md5_b": apk_b,
            "table": "all",
            "limit": 20,
        },
        label="GET /api/diff (container pair, child rows)",
    )
    items = (page or {}).get("items") or []
    matched = next((i for i in items if i.get("state") == "matched"), None)
    check(
        "a container pair pages its child files, with the path each sat at",
        isinstance(page, dict)
        and page.get("is_container_pair") is True
        and isinstance(matched, dict)
        and matched.get("path_in_parent_a") == "lib/one.so"
        and (matched.get("similarity") or 0) > 0,
        str(matched)[:250],
    )
    check(
        "child rows carry file metadata, not function metadata",
        isinstance(page, dict) and bool(page.get("files_metadata")),
        str((page or {}).get("files_metadata"))[:200],
    )

    requests.post(
        f"{BASE_URL}/api/collection/delete", json={"collection": coll}, timeout=60
    )


# ---------------------------------------------------------------------------
# Step 4c – Library tags roll up from functions to their file
#
# Tagging a function `lib:uclibc:0.9.30.1:xdrmem_getint32` means the binary
# contains uClibc, so the file must carry `lib:uclibc` -- without the version,
# which one matched function does not establish for the whole binary.
# Seeded synthetically because the roll-up rules have to hold whether or not
# Ghidra's Function ID databases happen to match anything in the test corpus;
# the uploaded collection is then checked for consistency on top.
# ---------------------------------------------------------------------------
def test_lib_tag_rollup():
    import subprocess
    from bsimvis.app.services.redis_client import get_redis
    from backfill_lib_file_tags import file_lib_tags

    print(_color(f"\n{'='*60}", CYAN))
    print(_color(" STEP 4c – Function library tags roll up to the file", BOLD))
    print(_color(f"{'='*60}", CYAN))

    r = get_redis()
    coll = f"{COLLECTION}_libtag"
    md5 = "0" * 32
    file_id = f"{coll}:file:{md5}"

    seeded = {
        "00401000": ["lib:uclibc:0.9.30.1:xdrmem_getint32"],
        # `ambiguous` is still evidence the library is present, and a second
        # library in the same file must not shadow the first.
        "00401100": ["lib:uclibc:0.9.30.1:ambiguous", "lib:musl:1.2.4"],
        # The unversioned lib tag rolls up like any other -- it still names a
        # library the binary contains. A plain tag is not a library at all.
        "00401200": ["lib:zlib:deflate", "crypto"],
    }
    expected = {"lib:uclibc", "lib:musl", "lib:zlib"}

    try:
        r.set(
            f"{file_id}:meta",
            json.dumps(
                {
                    "file_md5": md5,
                    "file_name": "synthetic_libtag",
                    "type": "file",
                    "collection": coll,
                    "file_id": file_id,
                    "tags": ["preexisting"],
                }
            ),
        )
        r.sadd(f"{coll}:all_files", file_id)
        for addr, tags in seeded.items():
            func_id = f"{coll}:func:{md5}:{addr}"
            r.set(
                f"{func_id}:meta",
                json.dumps({"function_name": f"f_{addr}", "tags": tags}),
            )
            r.sadd(f"{coll}:idx:file:functions:{md5}", func_id)

        check(
            "derives unversioned lib: tags from function tags",
            file_lib_tags(r, coll, md5) == expected,
            f"got {sorted(file_lib_tags(r, coll, md5))}, want {sorted(expected)}",
        )

        script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "backfill_lib_file_tags.py"
        )
        proc = subprocess.run(
            [sys.executable, script, "--collection", coll],
            capture_output=True,
            text=True,
            timeout=120,
        )
        check(
            "backfill script exits cleanly",
            proc.returncode == 0,
            (proc.stderr or proc.stdout)[-400:],
        )

        doc = r.get(f"{file_id}:meta")
        tags = json.loads(doc.decode() if isinstance(doc, bytes) else doc).get("tags")
        check(
            "backfill adds the library tags to the file doc",
            expected <= set(tags or []),
            f"file tags: {tags}",
        )
        check(
            "backfill keeps the file's pre-existing tags",
            "preexisting" in (tags or []),
            f"file tags: {tags}",
        )
        check(
            "backfill rolls up neither versions nor non-lib tags",
            not (
                {"lib:uclibc:0.9.30.1", "lib:zlib:deflate", "crypto"} & set(tags or [])
            ),
            f"file tags: {tags}",
        )
        check(
            "rolled-up tag is searchable at the file level",
            r.sismember(f"{coll}:idx:file:tags:lib:uclibc", file_id),
            f"{coll}:idx:file:tags:lib:uclibc",
        )

        # Idempotent: a second run must not duplicate anything.
        subprocess.run(
            [sys.executable, script, "--collection", coll],
            capture_output=True,
            text=True,
            timeout=120,
        )
        doc2 = r.get(f"{file_id}:meta")
        tags2 = json.loads(doc2.decode() if isinstance(doc2, bytes) else doc2).get(
            "tags"
        )
        check(
            "backfill is idempotent",
            tags2 == tags,
            f"first: {tags} / second: {tags2}",
        )

        # Ingest path: INDEX_FUNCTIONS accumulates, INDEX_FEATURES folds in.
        # Driven directly because Ghidra's Function ID databases match nothing
        # in the test corpus, so an upload alone never exercises this.
        from bsimvis.app.services.processing_service import ProcessingService

        ing_md5 = "1" * 32
        ing_id = f"{coll}:file:{ing_md5}"
        r.set(
            f"{ing_id}:meta",
            json.dumps({"file_md5": ing_md5, "type": "file", "file_id": ing_id}),
        )
        r.sadd(f"{coll}:all_files", ing_id)
        svc = ProcessingService(r)
        svc.index_functions(
            coll,
            None,
            functions_list=[
                {
                    "function_metadata": {
                        "function_name": f"f_{addr}",
                        "full_id": f"{ing_md5}:#{ing_md5}::f_{addr}:@{addr}",
                        "tags": tags,
                    }
                }
                for addr, tags in seeded.items()
            ],
            file_meta={"file_md5": ing_md5},
            file_md5=ing_md5,
        )
        check(
            "INDEX_FUNCTIONS accumulates the library tags of its chunk",
            {
                _s_tag.decode() if isinstance(_s_tag, bytes) else _s_tag
                for _s_tag in r.smembers(f"{ing_id}:lib_tags")
            }
            == expected,
            f"{ing_id}:lib_tags",
        )
        svc.rollup_lib_tags(coll, ing_md5)
        ing_doc = r.get(f"{ing_id}:meta")
        ing_tags = json.loads(
            ing_doc.decode() if isinstance(ing_doc, bytes) else ing_doc
        ).get("tags")
        check(
            "ingest path tags the file with its functions' libraries",
            set(ing_tags or []) == expected,
            f"file tags: {ing_tags}",
        )

        # And the real uploaded collection must already satisfy the same rule
        # through the ingest path (no-op when Function ID matched nothing).
        missing = []
        found_any = False
        for raw_fid in r.smembers(f"{COLLECTION}:all_files"):
            fid = raw_fid.decode() if isinstance(raw_fid, bytes) else raw_fid
            f_md5 = fid.split(":")[-1]
            derived = file_lib_tags(r, COLLECTION, f_md5)
            if not derived:
                continue
            found_any = True
            f_doc = r.get(f"{fid}:meta")
            if not f_doc:
                continue
            f_tags = set(
                json.loads(f_doc.decode() if isinstance(f_doc, bytes) else f_doc).get(
                    "tags"
                )
                or []
            )
            if derived - f_tags:
                missing.append(f"{f_md5}: {sorted(derived - f_tags)}")
        check(
            "uploaded collection: every function library tag reached its file",
            not missing,
            (
                "; ".join(missing)
                if missing
                else (
                    "checked live files" if found_any else "no Function ID lib matches"
                )
            ),
        )
    finally:
        keys = list(r.scan_iter(match=f"{coll}:*", count=1000))
        if keys:
            r.delete(*keys)


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

    # run_all_tests() stays last: it deletes the collection on its way out.
    STEPS = [
        test_pool_annotation_propagation,
        test_search_filters_and_sorting,
        test_tag_vocabulary_and_llm_batch,
        test_bin_sim_diff_cache,
        test_pool_collection_equivalence,
        test_archive_upload,
        test_unpack_upload,
        test_lineage,
        test_container_similarity,
        test_lib_tag_rollup,
        run_all_tests,
    ]

    # Not every step is self-contained: some read state an earlier one creates.
    # --only pulls those in, because a filter that reports a failure the full run
    # does not have is worse than no filter. A step that passes in a full run but
    # fails under --only means its entry here is missing.
    STEP_DEPS = {
        # Issues the /api/bin_sim/build whose doc the diff cache step reads.
        "test_bin_sim_diff_cache": ["test_search_filters_and_sorting"],
    }

    # Resolved before the prelude: a mistyped --only should fail now, not after
    # several minutes of uploading and analysing binaries.
    steps = STEPS
    if ONLY:
        picked = {s.__name__ for s in STEPS if ONLY in s.__name__}
        if not picked:
            print(_color(f"\n  No step matches --only {ONLY!r}. Available:", RED))
            for s in STEPS:
                print(f"    {s.__name__}")
            sys.exit(2)

        pending = list(picked)
        while pending:
            for dep in STEP_DEPS.get(pending.pop(), []):
                if dep not in picked:
                    picked.add(dep)
                    pending.append(dep)

        # Declaration order, so run_all_tests still deletes the collection last.
        steps = [s for s in STEPS if s.__name__ in picked]
        print(_color(f"  --only {ONLY!r} → {', '.join(s.__name__ for s in steps)}", CYAN))

    uploaded = upload_and_start()
    if uploaded:
        wait_for_pipeline()
        test_duplicate_upload()
        upload_second_binary()

    resolve_ids()

    for step in steps:
        step()
    print_summary()
