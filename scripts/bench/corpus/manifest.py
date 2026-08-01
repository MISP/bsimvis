#!/usr/bin/env python3
"""Describe a built corpus: one JSON record per binary.

Reads what `build_corpus.sh` produced and records the facts the benchmark needs
later: md5 (BSimVis keys files by md5), the build coordinates encoded in the
filename, and the defined-symbol count -- the ceiling on how many functions
ingest can possibly recover, and the ground truth for retrieval.

Usage:
    scripts/bench/corpus/manifest.py [--out ~/data/bsim-bench-corpus]
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys

# filename convention: <project>-<os>-<arch>-<opt>-<link>[.exe]
NAME_RE = re.compile(r"^(?P<project>[a-z0-9]+)-(?P<target>[a-z0-9]+-[a-z0-9]+)-"
                     r"(?P<opt>O[0-9s])-(?P<link>dyn|static)(?P<ext>\.exe)?$")

NM = {
    "linux-x64": "x86_64-linux-gnu-nm",
    "linux-arm64": "aarch64-linux-gnu-nm",
    "linux-ppc64le": "powerpc64le-linux-gnu-nm",
    "linux-riscv64": "riscv64-linux-gnu-nm",
    "win-x64": "x86_64-w64-mingw32-nm",
    "win-x32": "i686-w64-mingw32-nm",
}

VERSIONS = {}


def load_versions(here):
    with open(os.path.join(here, "sources.txt")) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name, ver, _sha, _url = line.split()
            VERSIONS[name] = ver


def defined_functions(path, target):
    """Defined text symbols (nm type T/t), i.e. the functions ground truth uses."""
    nm = NM.get(target, "nm")
    try:
        out = subprocess.run([nm, "--defined-only", "-f", "posix", path],
                             capture_output=True, text=True, timeout=300)
    except FileNotFoundError:
        return None
    if out.returncode != 0:
        return None
    names = set()
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1] in ("T", "t"):
            names.add(parts[0])
    return sorted(names)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.environ.get(
        "CORPUS_ROOT", os.path.expanduser("~/data/bsim-bench-corpus")))
    ap.add_argument("--symbols", action="store_true",
                    help="also write symbols.json (binary -> [function names])")
    args = ap.parse_args()

    load_versions(here)
    bindir = os.path.join(args.out, "bin")
    records, symbols = [], {}

    for project in sorted(os.listdir(bindir)):
        pdir = os.path.join(bindir, project)
        if not os.path.isdir(pdir):
            continue
        for fname in sorted(os.listdir(pdir)):
            m = NAME_RE.match(fname)
            if not m:
                print(f"skip (unparsable name): {fname}", file=sys.stderr)
                continue
            path = os.path.join(pdir, fname)
            blob = open(path, "rb").read()
            target = m.group("target")
            funcs = defined_functions(path, target)
            rec = {
                "file": fname,
                "path": path,
                "md5": hashlib.md5(blob).hexdigest(),
                "sha256": hashlib.sha256(blob).hexdigest(),
                "size": len(blob),
                "project": m.group("project"),
                "version": VERSIONS.get(m.group("project"), "?"),
                "target": target,
                "os": "windows" if target.startswith("win") else "linux",
                "arch": target.split("-", 1)[1],
                "opt": m.group("opt"),
                "link": m.group("link"),
                "defined_functions": len(funcs) if funcs is not None else None,
            }
            records.append(rec)
            if args.symbols and funcs:
                symbols[fname] = funcs

    manifest = os.path.join(args.out, "manifest.json")
    with open(manifest, "w") as fh:
        json.dump({"corpus_root": args.out, "binaries": records}, fh, indent=1)
    if args.symbols:
        with open(os.path.join(args.out, "symbols.json"), "w") as fh:
            json.dump(symbols, fh)

    total_funcs = sum(r["defined_functions"] or 0 for r in records)
    print(f"{len(records)} binaries, {sum(r['size'] for r in records) / 1e6:.0f} MB, "
          f"{total_funcs} defined functions -> {manifest}")
    by = {}
    for r in records:
        by.setdefault(r["project"], [0, 0])
        by[r["project"]][0] += 1
        by[r["project"]][1] += r["defined_functions"] or 0
    for p, (n, f) in sorted(by.items()):
        print(f"  {p:<8} {n:>4} binaries  {f:>7} functions")


if __name__ == "__main__":
    main()
