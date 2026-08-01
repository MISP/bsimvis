#!/usr/bin/env python3
"""Ghidra's own BSim database as a retrieval baseline on the same corpus.

BSimVis scores every candidate pair; Ghidra's BSim first narrows candidates with
LSH binning and only then scores. `scripts/bench/oracle_compare.py` already proves
our arithmetic matches Ghidra's -- what it cannot show is what that LSH stage
costs in recall. This runs the real thing end to end:

    createdatabase -> import + analyze in Ghidra -> generatesigs --commit
      -> per-query-binary BSimQueryAll.java -> recall@1 / recall@5 / MRR

so its numbers sit next to quality.py's on the same binaries and the same
symbol-name ground truth.

**Benchmark-only.** Everything here runs out of the Ghidra install already in
`bin/` via `support/bsim` and `support/analyzeHeadless`. No BSim database, driver
or dependency is added to BSimVis itself -- the application never talks to one.

Usage:
    scripts/bench/bsim_baseline.py --project sqlite --ref-target linux-x64 \
        --query-target linux-arm64 [--opt O2] [--link dyn] [--max-matches 10]
"""

import argparse
import collections
import glob
import json
import os
import shutil
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GHIDRA = os.environ.get("GHIDRA_INSTALL_DIR", os.path.join(REPO, "bin", "ghidra_12.1_PUBLIC"))
BSIM = os.path.join(GHIDRA, "support", "bsim")
HEADLESS = os.path.join(GHIDRA, "support", "analyzeHeadless")
SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bsim")


def run(cmd, **kw):
    print("$", " ".join(str(c) for c in cmd), flush=True)
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if proc.returncode != 0:
        print(proc.stdout[-3000:])
        print(proc.stderr[-3000:], file=sys.stderr)
        raise SystemExit(f"command failed ({proc.returncode})")
    return time.time() - t0, proc.stdout


def select(manifest, project, target, opt, link):
    return [r for r in manifest["binaries"]
            if r["project"] in project and r["target"] == target
            and r["opt"] == opt and r["link"] == link]


def metrics(query_json, expected_exe):
    """recall@1 / recall@5 / MRR over symbol-name ground truth."""
    data = json.load(open(query_json))
    ranks = []
    for res in data["results"]:
        name = res["function"]
        gold = [i for i, m in enumerate(res["matches"], 1)
                if m["name"] == name and m["exe"] == expected_exe]
        if not gold:
            continue  # true match not in the returned window at all
        ranks.append(min(gold))
    considered = len(data["results"])
    if not considered:
        return None
    found = len(ranks)
    return {
        "functions_queried": data["queried_functions"],
        "functions_with_results": considered,
        "true_match_returned": found / considered,
        "recall@1": sum(r == 1 for r in ranks) / considered,
        "recall@5": sum(r <= 5 for r in ranks) / considered,
        "mrr": sum(1 / r for r in ranks) / considered,
        "query_millis": data["query_millis"],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.environ.get(
        "CORPUS_ROOT", os.path.expanduser("~/data/bsim-bench-corpus")))
    ap.add_argument("--project", action="append", default=[],
                    help="corpus projects to include (default: all)")
    ap.add_argument("--ref-target", default="linux-x64")
    ap.add_argument("--query-target", default="linux-arm64")
    ap.add_argument("--opt", default="O2")
    ap.add_argument("--link", default="dyn")
    ap.add_argument("--config", default="medium_nosize",
                    help="BSim template; medium_nosize matches the 0x4D profile")
    ap.add_argument("--max-matches", type=int, default=10)
    ap.add_argument("--sim-threshold", type=float, default=0.0)
    ap.add_argument("--sig-threshold", type=float, default=0.0)
    ap.add_argument("--keep", action="store_true", help="reuse an existing db/project")
    ap.add_argument("--report", default=None)
    args = ap.parse_args()

    manifest = json.load(open(os.path.join(args.out, "manifest.json")))
    projects = set(args.project) or {r["project"] for r in manifest["binaries"]}
    refs = select(manifest, projects, args.ref_target, args.opt, args.link)
    queries = select(manifest, projects, args.query_target, args.opt, args.link)
    if not refs or not queries:
        sys.exit("nothing selected -- check --project/--ref-target/--query-target")

    work = os.path.join(args.out, "bsim")
    db_dir = os.path.join(work, "db")
    proj_dir = os.path.join(work, "project")
    out_dir = os.path.join(work, "queries")
    if not args.keep:
        shutil.rmtree(work, ignore_errors=True)
    for d in (db_dir, proj_dir, out_dir):
        os.makedirs(d, exist_ok=True)

    db_url = f"file:{db_dir}/bench"
    timings = {}

    if not args.keep or not glob.glob(os.path.join(db_dir, "*")):
        timings["createdatabase"], _ = run([BSIM, "createdatabase", db_url, args.config])

    # Reference and query binaries live in separate project folders so signatures
    # are committed for the reference side only.
    timings["import_refs"], _ = run(
        [HEADLESS, proj_dir, "bench/refs", "-import"] + [r["path"] for r in refs]
        + ["-noanalysis"])
    timings["analyze_refs"], _ = run(
        [HEADLESS, proj_dir, "bench/refs", "-process", "-recursive"])
    timings["generatesigs"], _ = run(
        [BSIM, "generatesigs", f"ghidra:{proj_dir}/bench?/refs", "--bsim", db_url, "--commit"])

    timings["import_queries"], _ = run(
        [HEADLESS, proj_dir, "bench/queries", "-import"] + [q["path"] for q in queries]
        + ["-noanalysis"])
    timings["analyze_queries"], _ = run(
        [HEADLESS, proj_dir, "bench/queries", "-process", "-recursive"])

    results = {}
    for q in queries:
        # the reference build of the same project is the one holding true matches
        ref = next((r for r in refs if r["project"] == q["project"]), None)
        if ref is None:
            continue
        out_json = os.path.join(out_dir, f"{q['file']}.json")
        t, _ = run([HEADLESS, proj_dir, "bench/queries", "-process", q["file"],
                    "-noanalysis",
                    "-scriptPath", SCRIPTS,
                    "-postScript", "BSimQueryAll.java", db_url, out_json,
                    str(args.max_matches), str(args.sim_threshold), str(args.sig_threshold)])
        m = metrics(out_json, ref["file"])
        if m:
            m["seconds"] = t
            m["reference"] = ref["file"]
            results[q["file"]] = m

    if not results:
        sys.exit("no query produced results")

    print(f"\n{'query binary':<40}{'in window':>11}{'recall@1':>10}{'recall@5':>10}{'MRR':>8}")
    for name, m in sorted(results.items()):
        print(f"{name:<40}{m['true_match_returned']:>10.1%}{m['recall@1']:>10.1%}"
              f"{m['recall@5']:>10.1%}{m['mrr']:>8.3f}")
    mean = {k: sum(m[k] for m in results.values()) / len(results)
            for k in ("true_match_returned", "recall@1", "recall@5", "mrr")}
    print(f"{'MEAN':<40}{mean['true_match_returned']:>10.1%}{mean['recall@1']:>10.1%}"
          f"{mean['recall@5']:>10.1%}{mean['mrr']:>8.3f}")
    print("\n'in window' = share of queries whose true match appeared at all in the "
          f"top {args.max_matches} BSim returned -- i.e. survived LSH candidate selection.")

    if args.report:
        with open(args.report, "w") as fh:
            json.dump({"config": vars(args), "timings": timings,
                       "results": results, "mean": mean}, fh, indent=1)
        print(f"wrote {args.report}")


if __name__ == "__main__":
    main()
