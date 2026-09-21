"""`bsimvis scan` -- compare a file against existing collections, ingest nothing.

Thin client for /api/scan: POST the bytes, poll until the scan finishes, print
the match table.
"""

import os
import time

import requests


def _fmt(value, width=6):
    return "-".rjust(width) if value is None else f"{float(value):.4f}".rjust(width)


def _print_report(doc):
    print(f"\n[+] Scan {doc['scan_id']}  {doc.get('file_name')}  {doc['file_md5']}")
    print(f"    {doc.get('function_count', 0)} functions analysed")
    for warning in doc.get("warnings") or []:
        print(f"[!] {warning}")
    if doc.get("already_present"):
        print(f"[*] Already present in: {', '.join(doc['already_present'])}")

    for scope in doc.get("scanned") or []:
        print(
            f"\n  {scope['collection']}  "
            f"({scope['files_scored']} scored of {scope['files_touched']} touched)"
        )
        if not scope["files"]:
            print("    no matches")
        else:
            print(
                f"    {'score':>6} {'code':>6} {'lib':>6} {'cov':>6}  "
                f"{'funcs':>6}  md5                               name"
            )
            for row in scope["files"]:
                print(
                    f"    {_fmt(row.get('score'))} {_fmt(row.get('score_code'))} "
                    f"{_fmt(row.get('score_library'))} {_fmt(row.get('coverage_a'))}  "
                    f"{str(row.get('matched_functions', 0)).rjust(6)}  "
                    f"{row['file_md5']}  {row.get('file_name', '')}"
                )
        for axis, clusters in (scope.get("bin_clusters") or {}).items():
            for cluster in clusters:
                print(
                    f"    would join [{axis}] {cluster.get('cluster_name') or cluster['cluster_uuid']} "
                    f"({cluster.get('member_count', 0)} members)"
                )


def run_scan(host, port, args):
    api_url = f"http://{host}:{port}/api/scan"

    path = args.file
    if not os.path.isfile(path):
        print(f"[!] Not a file: {path}")
        return 1
    with open(path, "rb") as f:
        raw = f.read()

    params = [("file_name", os.path.basename(path))]
    for coll in args.collection or []:
        params.append(("collection", coll))
    for pool in args.pool or []:
        params.append(("pool", pool))
    if args.all:
        params.append(("all", "true"))
    for name in ("algo", "min_score", "min_features", "top_files", "profile"):
        value = getattr(args, name, None)
        if value is not None:
            params.append((name, str(value)))
    for module in args.enable or []:
        params.append(("enable", module))

    print(f"[*] Scanning {os.path.basename(path)} ({len(raw)} bytes)...")
    resp = requests.post(
        api_url,
        params=params,
        data=raw,
        headers={"Content-Type": "application/octet-stream"},
    )
    if resp.status_code >= 400:
        print(f"[!] Scan rejected: {resp.text}")
        return 1
    scan_id = resp.json()["scan_id"]
    print(f"[*] Scan {scan_id} queued against {', '.join(resp.json()['scopes'])}")

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        time.sleep(2)
        doc = requests.get(f"{api_url}/{scan_id}").json()
        status = doc.get("status")
        if status == "completed":
            _print_report(doc)
            return 0
        if status == "failed" or doc.get("job_status") == "failed":
            print(f"[!] Scan failed: {doc.get('error', 'unknown error')}")
            return 1
        print(f"    {status} {doc.get('progress', 0)}%", end="\r", flush=True)

    print(f"\n[!] Timed out after {args.timeout}s; poll {api_url}/{scan_id}")
    return 1
