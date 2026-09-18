#!/usr/bin/env python3
"""Go/no-go gate for BSim feature weighting (MISP/bsimvis#30).

Ghidra's IDF table lists the 1000 most common feature hashes in *its* training
corpus, which is predominantly x86 C/C++. This collection is heavily Go on
MIPS/ARM/SH4/m68k. A hash absent from that table resolves to lookup index 0,
which carries the *largest* weight -- so if our boilerplate is missing from
Ghidra's list, weighting would amplify exactly the features it is meant to
suppress.

This measures that before any scorer is written. It reads only keys that already
exist -- per-feature document frequency is materialized as
`{coll}:feature:{hash}:functions` -- so it is a read loop, not a corpus scan.

Usage:
    scripts/bench/idf_coverage.py <collection> [--top 50] [--profile nosize]
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from bsimvis.app.services import bsim_profiles, bsim_weights  # noqa: E402
from bsimvis.app.services.redis_client import get_redis  # noqa: E402


def collect_document_frequencies(r, collection):
    """{hash_int: document_frequency} from the materialized posting lists."""
    dfs = {}
    pattern = f"{collection}:feature:*:functions"
    batch = []

    def flush(keys):
        if not keys:
            return
        pipe = r.pipeline(transaction=False)
        for k in keys:
            pipe.zcard(k)
        for k, count in zip(keys, pipe.execute()):
            name = k.decode() if isinstance(k, bytes) else k
            # {coll}:feature:{hash}:functions
            f_hash = name.split(":")[-2]
            try:
                dfs[int(f_hash, 16)] = count
            except ValueError:
                continue

    for key in r.scan_iter(match=pattern, count=1000):
        batch.append(key)
        if len(batch) >= 1000:
            flush(batch)
            batch = []
    flush(batch)
    return dfs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("collection")
    ap.add_argument("--top", type=int, default=50, help="how many high-DF hashes to list")
    ap.add_argument("--profile", default=None, help="BSim profile (default: configured)")
    args = ap.parse_args()

    profile = bsim_profiles.get_profile(args.profile)
    table = bsim_weights.load(profile.weights_path)
    print(f"profile        : {profile.name} (settings {profile.settings:#x})")
    print(f"weights table  : {profile.weights_path}")
    print(f"idflookup size : {len(table.lookup)}")

    r = get_redis()
    dfs = collect_document_frequencies(r, args.collection)
    if not dfs:
        print(f"\nNo features found for collection {args.collection!r}.")
        return 1

    total_hashes = len(dfs)
    total_occurrences = sum(dfs.values())
    covered = {h: df for h, df in dfs.items() if h in table.lookup}
    covered_occurrences = sum(covered.values())

    print(f"\ncollection     : {args.collection}")
    print(f"distinct hashes: {total_hashes}")
    print(f"total feature occurrences (sum of DF): {total_occurrences}")

    print("\n--- Coverage by Ghidra's idflookup ---")
    print(
        f"distinct hashes covered   : {len(covered)}/{total_hashes} "
        f"({100.0 * len(covered) / total_hashes:.2f}%)"
    )
    # This is the number that matters: a table can cover few distinct hashes and
    # still cover most of what actually occurs, or the reverse.
    print(
        f"OCCURRENCES covered       : {covered_occurrences}/{total_occurrences} "
        f"({100.0 * covered_occurrences / total_occurrences:.2f}%)"
    )

    max_weight = table.idfweight[0]
    print(f"\n--- Top {args.top} highest-DF hashes here ---")
    print(f"{'hash':>10}  {'DF':>8}  {'idx':>4}  {'weight':>8}  {'w/max':>6}  status")
    ranked = sorted(dfs.items(), key=lambda kv: kv[1], reverse=True)[: args.top]
    unknown_in_top = 0
    for h, df in ranked:
        idx = table.lookup.get(h)
        if idx is None:
            idx_s, status = "-", "ABSENT -> MAX WEIGHT"
            weight = max_weight
            unknown_in_top += 1
        else:
            idx_s, status = str(idx), "known"
            weight = table.idfweight[idx]
        print(
            f"0x{h:08x}  {df:8d}  {idx_s:>4}  {weight:8.4f}  "
            f"{weight / max_weight:6.3f}  {status}"
        )

    print(
        f"\n{unknown_in_top}/{len(ranked)} of this collection's most common features are "
        f"ABSENT from Ghidra's table"
    )
    print("and would therefore receive the maximum weight.")
    print(
        "\nVerdict: the higher that count and the lower the occurrence coverage, the more "
        "\nGhidra's shipped table misreads this corpus. If most top-DF features are absent, "
        "\nadopt a table derived from an independent reference pool instead "
        "(scripts/bench/make_weights.py) rather than the shipped one."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
