#!/usr/bin/env python3
"""Ghidra-extract the corpus once, and record how long that took.

Writes vectors/<md5>.json -- {function_name: {feature_hash: tf}} -- which is what
quality.py scores offline, with no server and no database involved. Ghidra runs
once for the whole corpus; every later accuracy rerun is seconds.

Ghidra decompilation dominates end-to-end ingest wall time, so its per-binary
cost is a benchmark result in itself and is written to extract_times.json.

Usage:
    scripts/bench/corpus/extract.py [--out ~/data/bsim-bench-corpus] [--jobs 4]
                                    [--only sqlite,zlib] [--match REGEX] [--keep-dumps]

Extraction never needs the stack: it runs `bsimvis upload --local-analysis
--no-upload --save-vectors`, which analyses locally and writes the vectors
without touching the API. Re-running is safe and cheap -- a binary whose vectors
already exist is skipped, so an interrupted pass resumes where it stopped.

On a headless machine, run with DISPLAY= : Ghidra's project API tries to reach an
X server otherwise and every binary fails in under a second.
"""

import argparse
import ast
import collections
import concurrent.futures
import hashlib
import json
import os
import re
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def vectorize(dump_path, out_path):
    """Collapse a dump to {function_name: {hash: tf}} and note duplicate names."""
    with open(dump_path) as fh:
        data = json.load(fh)
    def as_dict(v):
        # Dumps written locally hold real dicts; ones that went through the API
        # come back as python-repr strings. Accept both.
        if isinstance(v, dict):
            return v
        return ast.literal_eval(v)

    vectors, meta = {}, {}
    for fn in data.get("functions", []):
        try:
            fmeta = as_dict(fn["function_metadata"])
            feats = as_dict(fn["function_features"])
        except (ValueError, SyntaxError, KeyError, TypeError):
            continue
        name = fmeta.get("function_name")
        if not name:
            continue
        tf = collections.Counter(
            f["hash"] for f in feats.get("bsim_features_meta", []) if f.get("hash")
        )
        if not tf:
            continue
        # A name can repeat (static functions from different objects). Keep the
        # first and count the rest -- ground truth cannot disambiguate them.
        if name in vectors:
            meta.setdefault("duplicate_names", []).append(name)
            continue
        vectors[name] = dict(tf)
    meta["file_md5"] = data.get("file_md5")
    meta["file_name"] = data.get("file_metadata", {}).get("file_name")
    meta["n_functions"] = len(vectors)
    with open(out_path, "w") as fh:
        json.dump({"meta": meta, "vectors": vectors}, fh)
    return meta


def extract_one(binary, dumps, vectors_dir, keep_dumps, known_md5=None, force=False):
    name = os.path.basename(binary)
    # Resume: Ghidra is the expensive part, never redo a binary already vectorized.
    if known_md5 and not force:
        done = os.path.join(vectors_dir, f"{known_md5}.json")
        if os.path.exists(done):
            n = json.load(open(done))["meta"].get("n_functions", 0)
            return {"file": name, "ok": True, "seconds": 0.0, "md5": known_md5,
                    "functions": n, "skipped": True}
    cmd = ["uv", "run", "bsimvis", "upload", "--local-analysis", "--no-upload",
           "--save-vectors", vectors_dir, "-c", "bench", binary]
    if keep_dumps:
        cmd[6:6] = ["--save-json", dumps]

    t0 = time.time()
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    elapsed = time.time() - t0

    # Output is named by md5, which the manifest already knows -- do NOT go
    # looking for "the newest file": with --jobs > 1 that attributes another
    # worker's output to this binary.
    md5 = known_md5 or hashlib.md5(open(binary, "rb").read()).hexdigest()
    vpath = os.path.join(vectors_dir, f"{md5}.json")
    if not os.path.exists(vpath):
        return {"file": name, "ok": False, "seconds": elapsed,
                "error": (proc.stderr or "")[-400:]}

    n = json.load(open(vpath))["meta"].get("n_functions", 0)
    print(f"[ok] {name:<44} {elapsed:7.1f}s  {n:>5} funcs")
    return {"file": name, "ok": True, "seconds": elapsed, "md5": md5,
            "functions": n}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.environ.get(
        "CORPUS_ROOT", os.path.expanduser("~/data/bsim-bench-corpus")))
    ap.add_argument("--jobs", type=int, default=4,
                    help="parallel Ghidra processes (each holds its own JVM)")
    ap.add_argument("--only", help="comma-separated project names")
    ap.add_argument("--match", help="regex on the binary filename, e.g. "
                                    "'-(linux-x64|win-x64)-O2-' to extract one slice first")
    ap.add_argument("--force", action="store_true",
                    help="re-extract even if vectors exist (e.g. to keep dumps this time)")
    ap.add_argument("--keep-dumps", action="store_true",
                    help="also write the full upload payload per binary. These run to "
                         "several GB each for a static build -- only for replay work")
    args = ap.parse_args()

    manifest_path = os.path.join(args.out, "manifest.json")
    if not os.path.exists(manifest_path):
        sys.exit(f"no manifest at {manifest_path} -- run corpus/manifest.py first")
    binaries = json.load(open(manifest_path))["binaries"]
    if args.only:
        wanted = set(args.only.split(","))
        binaries = [b for b in binaries if b["project"] in wanted]
    if args.match:
        pat = re.compile(args.match)
        binaries = [b for b in binaries if pat.search(b["file"])]

    dumps = os.path.join(args.out, "dumps")
    vectors_dir = os.path.join(args.out, "vectors")
    os.makedirs(dumps, exist_ok=True)
    os.makedirs(vectors_dir, exist_ok=True)

    print(f"extracting {len(binaries)} binaries with {args.jobs} workers")
    results = []
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futs = [pool.submit(extract_one, b["path"], dumps, vectors_dir,
                            args.keep_dumps, b.get("md5"), args.force)
                for b in binaries]
        for f in concurrent.futures.as_completed(futs):
            results.append(f.result())

    wall = time.time() - t0
    ok = [r for r in results if r["ok"]]
    out = {
        "wall_seconds": wall,
        "jobs": args.jobs,
        "binaries": len(results),
        "ok": len(ok),
        "total_functions": sum(r.get("functions", 0) for r in ok),
        "cpu_seconds": sum(r["seconds"] for r in results),
        "results": sorted(results, key=lambda r: -r["seconds"]),
    }
    # Merge with earlier passes: extraction is usually run in slices, and the
    # per-binary Ghidra cost of a slice already done is still a result.
    times_path = os.path.join(args.out, "extract_times.json")
    merged = {r["file"]: r for r in out["results"]}
    if os.path.exists(times_path):
        for r in json.load(open(times_path)).get("results", []):
            merged.setdefault(r["file"], r)
    out["results"] = sorted(merged.values(), key=lambda r: -r["seconds"])
    with open(times_path, "w") as fh:
        json.dump(out, fh, indent=1)
    print(f"\n{len(ok)}/{len(results)} extracted in {wall / 60:.1f} min wall "
          f"({out['cpu_seconds'] / 60:.1f} min of Ghidra), "
          f"{out['total_functions']} functions")
    for r in results:
        if not r["ok"]:
            print(f"  FAILED {r['file']}: {r.get('error', '')[:200]}")


if __name__ == "__main__":
    main()
