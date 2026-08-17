#!/usr/bin/env python3
"""Compare build_sim throughput across a deploy, on the SAME in-flight jobs.

Why this and not the pipeline benchmark: build_sim speed depends heavily on the
binary and on how large the collection already is, so two different jobs are not
comparable. A job that is running before the deploy and resumes after it holds
both constant - same binary, same corpus - which makes the delta attributable to
the code change.

  # before deploying
  python scripts/compare_build_speed.py snapshot before.json
  # deploy, then let the resumed jobs run past their previous high-water mark
  python scripts/compare_build_speed.py snapshot after.json
  python scripts/compare_build_speed.py compare before.json after.json

Read-only: only GETs the jobs API. Host via --api (default http://localhost:5000).

Caveat the compare step enforces: a requeued job restarts its loop at 0 and races
through already-built functions (one SISMEMBER each, no work). Those replayed
counts are not build throughput, so any job whose counter went backwards is
flagged and excluded until it passes where it was.
"""

import argparse
import json
import statistics
import time
import urllib.request


def fetch(api, path):
    with urllib.request.urlopen(api + path, timeout=40) as r:
        return json.load(r)


def as_int(v):
    """The jobs API returns counters as strings. Comparing them as strings is
    lexicographic ("9" > "10"), which silently breaks both the delta and the
    resumed-job detection, so everything is coerced on the way in."""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def snapshot(api, path):
    st = fetch(api, "/api/jobs/stats")
    out = {"wall": time.time(), "api": api, "jobs": {}}
    for j in st.get("active_jobs", []):
        if j.get("status") != "running" or j.get("type") != "build_sim":
            continue
        d = fetch(api, "/api/jobs/" + j["id"])
        pl = d.get("payload") or {}
        if isinstance(pl, str):
            try:
                pl = json.loads(pl)
            except ValueError:
                pl = {}
        out["jobs"][d["id"]] = {
            "md5": pl.get("md5"),
            "total": as_int(d.get("total_items")),
            "processed": as_int(d.get("processed_items")),
            "collection": d.get("collection"),
        }
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    print(f"snapshot: {len(out['jobs'])} running build_sim job(s) -> {path}")
    for jid, v in out["jobs"].items():
        print(f"  {jid[:8]} md5={str(v['md5'])[:12]} {v['processed']}/{v['total']}")


def compare(before, after):
    a = json.load(open(before))
    b = json.load(open(after))
    dt = b["wall"] - a["wall"]
    print(f"elapsed between snapshots: {dt:,.0f}s\n")
    print(f"{'job':10} {'md5':14} {'total':>9} {'built':>9} {'fn/s':>9}  note")
    print("-" * 68)
    rates = []
    for jid, av in a["jobs"].items():
        bv = b["jobs"].get(jid)
        md5 = str(av.get("md5"))[:12]
        if not bv:
            print(
                f"{jid[:8]:10} {md5:14} {as_int(av['total']):>9,} {'-':>9} {'-':>9}  "
                f"finished or not running"
            )
            continue
        ap, bp = as_int(av.get("processed")), as_int(bv.get("processed"))
        if bp < ap:
            print(
                f"{jid[:8]:10} {md5:14} {as_int(av['total']):>9,} {bp:>9,} {'-':>9}  "
                f"RESUMED - replaying skip-set, wait until it passes {ap:,}"
            )
            continue
        dp = bp - ap
        if dp <= 0 or dt <= 0:
            print(
                f"{jid[:8]:10} {md5:14} {as_int(av['total']):>9,} {bp:>9,} {'-':>9}  "
                f"no progress"
            )
            continue
        rate = dp / dt
        rates.append(rate)
        print(f"{jid[:8]:10} {md5:14} {as_int(av['total']):>9,} {dp:>9,} {rate:>9.4f}")
    if rates:
        print(
            f"\nmedian fn/s over {len(rates)} comparable job(s): "
            f"{statistics.median(rates):.4f}"
        )
        print("Compare this against the same figure from two pre-deploy snapshots.")
    else:
        print(
            "\nNo comparable jobs. Either everything was requeued (wait longer) "
            "or the jobs completed - in that case compare jobs with equal "
            "total_items instead, which on a duplicate-heavy corpus are usually "
            "near-identical binaries."
        )


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--api", default="http://localhost:5000", help="API base URL")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot", help="record in-flight build_sim progress")
    s.add_argument("path")
    c = sub.add_parser("compare", help="diff two snapshots into fn/s")
    c.add_argument("before")
    c.add_argument("after")
    a = p.parse_args()
    if a.cmd == "snapshot":
        snapshot(a.api.rstrip("/"), a.path)
    else:
        compare(a.before, a.after)


if __name__ == "__main__":
    main()
