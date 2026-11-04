"""Measure what N-way bridges recover on a sampled set, against reading every pair.

Builds one file set several ways through the live API -- `pair_k=0` (every
pair read: the reference) and sampled with each bridge kind -- and prints, per
variant, how its rows compare with the reference:

  precision / recall  pairs of functions sharing a row (pair counting over the
                      contingency table, so no fid pair is ever materialised)
  split               reference rows of 2+ files whose functions land in 2+ rows
  rows, lanes         rows spanning 2+ files; sets of files held by 2+ such rows
  bridged             rows holding at least one bridge edge
  secs                POST /nway/build to a served doc (includes queue wait)

Large sets are built by NWAY_BUILD jobs, so a worker must be running. Each
variant pulls every row once (`limit=0`): on a set of thousands of functions
per file that is tens of MB of JSON per variant.

    uv run python scripts/bench_nway_bridges.py --collection C --md5s md5s.txt
    uv run python scripts/bench_nway_bridges.py --pool P --md5s md5s.txt \\
        --sample-above 50 --variants none,lane,lane+fid+cluster
"""

import argparse
import os
import time
from collections import Counter

import requests

VARIANTS = "none,lane,fid,cluster,lane+fid+cluster"


def fetch(api, args, timeout):
    """Rows of one built set; queues the build job when the set needs one."""
    start = time.time()
    url = f"{api}/api/bin_sim/nway"
    while True:
        resp = requests.get(url, params=args, timeout=600)
        if resp.status_code == 200:
            return resp.json(), time.time() - start
        body = resp.json() if resp.content else {}
        if resp.status_code != 428:
            raise SystemExit(f"HTTP {resp.status_code}: {str(body)[:300]}")
        if not body.get("job"):
            requests.post(f"{url}/build", params=args, timeout=600).raise_for_status()
        if time.time() - start > timeout:
            raise SystemExit(f"build timed out after {timeout}s")
        time.sleep(5)


def build_local(scope, collection, md5s, extra):
    """`fetch`, in this process: the base and rows `/nway` would serve, nothing cached."""
    from bsimvis.app.services import nway_diff_service as nds
    from bsimvis.app.services.redis_client import get_redis

    start = time.time()
    r = get_redis()
    members = [(collection, m) for m in md5s] if collection else []
    if not collection:
        raise SystemExit("--in-process needs --collection")
    members, labels_, warnings = nds.expand_members(r, members)
    sample = list(nds.pair_sample())
    sample[0] = extra.get("sample_above", sample[0])
    sample[1] = extra.get("pair_k", sample[1])
    if "bridges" in extra:
        sample[3] = nds.bridge_kinds(extra["bridges"])
    sample = tuple(sample) if sample[1] > 0 else None
    base = nds._load_base(r, scope, members, None, labels_, warnings, sample)
    rows = nds._match(base, nds.default_min_edge())
    return {"items": rows, "warnings": base["warnings"]}, time.time() - start


def labels(body):
    """{fid: row index} and the rows spanning 2+ files as fid lists."""
    rows = [
        [f for f in (i.get("files") or i["cells"]).values() if isinstance(f, str)]
        for i in body.get("items", [])
        if not i.get("header")
    ]
    return {f: n for n, fs in enumerate(rows) for f in fs}, [
        r for r in rows if len(r) > 1
    ]


def pairs(sizes):
    return sum(n * (n - 1) // 2 for n in sizes)


def compare(ref, test):
    ref_of, ref_rows = ref
    test_of, test_rows = test
    both = Counter((ref_of[f], test_of.get(f)) for f in ref_of)
    tp = pairs(n for (_, t), n in both.items() if t is not None)
    ref_pairs = pairs(len(r) for r in ref_rows)
    test_pairs = pairs(len(r) for r in test_rows)
    split = sum(len({test_of.get(f) for f in r}) > 1 for r in ref_rows)
    return {
        "precision": tp / test_pairs if test_pairs else 1.0,
        "recall": tp / ref_pairs if ref_pairs else 1.0,
        "split": split,
    }


def lanes(body):
    masks = Counter(
        frozenset(i["cells"])
        for i in body.get("items", [])
        if not i.get("header") and i.get("span", 0) > 1
    )
    return sum(1 for c in masks.values() if c >= 2)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--api", default=f"http://localhost:{os.getenv('APP_PORT', 5000)}")
    ap.add_argument("--collection")
    ap.add_argument("--pool")
    ap.add_argument("--md5s", required=True, help="file with one md5 per line")
    ap.add_argument("--sample-above", type=int, default=None)
    ap.add_argument("--pair-k", type=int, default=None)
    ap.add_argument("--variants", default=VARIANTS, help=f"default {VARIANTS}")
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument(
        "--in-process",
        action="store_true",
        help="build with this checkout's code straight from Kvrocks: no app, no job, "
        "nothing written (point KVROCKS_PORT at the stack; run where its config is)",
    )
    a = ap.parse_args()
    if not (a.collection or a.pool):
        ap.error("--collection or --pool")
    with open(a.md5s) as fh:
        md5s = [line.strip() for line in fh if line.strip()]
    base = {"md5s": ",".join(md5s), "tab": "all", "scope": "all", "limit": 0}
    base.update({"collection": a.collection} if a.collection else {"pool": a.pool})
    if a.in_process:
        scope = {"pool": a.pool} if a.pool else {"collection": a.collection}
        run = lambda extra: build_local(scope, a.collection, md5s, extra)
    else:
        run = lambda extra: fetch(a.api, {**base, **extra}, a.timeout)

    ref_body, secs = run({"pair_k": 0})
    ref = labels(ref_body)
    print(f"{len(md5s)} files, {len(ref[0]):,} functions")
    head = f"{'variant':<18}{'prec':>7}{'recall':>8}{'split':>7}{'rows':>8}{'lanes':>7}{'bridged':>9}{'secs':>8}"
    print(head, flush=True)
    print(
        f"{'all pairs (ref)':<18}{1:>7.3f}{1:>8.3f}{0:>7}{len(ref[1]):>8}"
        f"{lanes(ref_body):>7}{0:>9}{secs:>8.0f}",
        flush=True,
    )
    for v in a.variants.split(","):
        extra = {"bridges": v.replace("+", ",")}
        if a.sample_above is not None:
            extra["sample_above"] = a.sample_above
        if a.pair_k is not None:
            extra["pair_k"] = a.pair_k
        body, secs = run(extra)
        test = labels(body)
        m = compare(ref, test)
        bridged = sum(1 for i in body.get("items", []) if i.get("bridged"))
        print(
            f"{v:<18}{m['precision']:>7.3f}{m['recall']:>8.3f}{m['split']:>7}"
            f"{len(test[1]):>8}{lanes(body):>7}{bridged:>9}{secs:>8.0f}",
            flush=True,
        )
        for w in body.get("warnings") or []:
            if w.startswith(("Bridges", "Large set")):
                print(f"    {w}", flush=True)


if __name__ == "__main__":
    main()
