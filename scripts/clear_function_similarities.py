#!/usr/bin/env python3
"""Enqueue CLEAR_SIM jobs for every file in a collection.

/api/similarity/clear only accepts one md5 (or one batch) per call, so wiping a
whole collection's function-to-function similarities means one job per file.
This walks /api/features/files and posts a clear job for each md5.

Dry-run by default; pass --apply to actually enqueue.

Usage:
    python3 scripts/clear_function_similarities.py --collection NAME
    python3 scripts/clear_function_similarities.py --collection NAME --apply

Target host comes from --api or $BSIMVIS_API, default http://localhost:5000.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


def api(url, payload=None, timeout=30):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--api",
        default=os.environ.get("BSIMVIS_API", "http://localhost:5000"),
        help="BSimVis base URL (env: BSIMVIS_API)",
    )
    p.add_argument("--collection", required=True)
    p.add_argument("--algo", default="unweighted_cosine")
    p.add_argument("--apply", action="store_true", help="enqueue (default: dry run)")
    args = p.parse_args()

    base = args.api.rstrip("/")
    try:
        listing = api(
            f"{base}/api/features/files?collection={urllib.parse.quote(args.collection)}"
        )
    except (urllib.error.URLError, OSError) as e:
        sys.exit(f"cannot reach {base}: {e} (set --api or $BSIMVIS_API)")
    md5s = [f["file_md5"] for f in listing.get("results", []) if f.get("file_md5")]
    if not md5s:
        sys.exit(f"no files found in collection {args.collection!r} at {base}")

    print(f"{len(md5s)} files in {args.collection!r}, algo={args.algo}")
    if not args.apply:
        print("dry run, nothing enqueued. re-run with --apply")
        for m in md5s[:5]:
            print(f"  would clear {m}")
        if len(md5s) > 5:
            print(f"  ... and {len(md5s) - 5} more")
        return

    ok, failed = [], []
    for i, md5 in enumerate(md5s, 1):
        try:
            res = api(
                f"{base}/api/similarity/clear",
                {"collection": args.collection, "md5": md5, "algo": args.algo},
            )
            ok.append(res.get("job_id"))
            print(f"[{i}/{len(md5s)}] {md5} -> {res.get('job_id')}")
        except (urllib.error.URLError, OSError, ValueError) as e:
            failed.append((md5, str(e)))
            print(f"[{i}/{len(md5s)}] {md5} -> FAILED: {e}", file=sys.stderr)

    print(f"\nenqueued {len(ok)}, failed {len(failed)}")
    if failed:
        sys.exit(1)
    print(f"watch: curl -s '{base}/api/jobs?collection={args.collection}' | head -c 2000")


if __name__ == "__main__":
    main()
