"""Counts (and, with --delete --yes, drops) the plan F LLM capability records.

Plan G replaced them with analyst capabilities (global:capability:*). Old keys per
collection: `{coll}:capabilities` (hash), `{coll}:idx:capability:label:{id}` (sets,
one per catalog id, so no KEYS / SCAN of the keyspace) and the `caps` list on
`{coll}:nway_names` records. Pools are listed with --pool.

    uv run python -I scripts/capabilities_old_records.py            # dry run
    uv run python -I scripts/capabilities_old_records.py --delete --yes
"""

import argparse
import json
import sys

sys.path.insert(0, ".")
from bsimvis.app.services import capability_service  # noqa: E402
from bsimvis.app.services.redis_client import get_redis  # noqa: E402

dec = capability_service._decode


def scan(r, ns, delete):
    ids = list(capability_service.catalog()["mbc"]) + list(
        capability_service.catalog()["attack"]
    )
    pipe = r.pipeline(transaction=False)
    for i in ids:
        pipe.exists(f"{ns}:idx:capability:label:{i}")
    labels = [i for i, ok in zip(ids, pipe.execute()) if ok]
    recs = r.hlen(f"{ns}:capabilities")
    named = {}
    for k, v in r.hscan_iter(f"{ns}:nway_names"):
        rec = json.loads(dec(v))
        if rec.get("caps"):
            named[dec(k)] = rec
    print(
        f"{ns}: {recs} records, {len(labels)} label sets, {len(named)} names with caps"
    )
    if not delete:
        return
    pipe = r.pipeline(transaction=False)
    pipe.delete(f"{ns}:capabilities")
    for i in labels:
        pipe.delete(f"{ns}:idx:capability:label:{i}")
    for k, rec in named.items():
        rec["caps"] = []
        pipe.hset(f"{ns}:nway_names", k, json.dumps(rec))
    pipe.execute()
    print(f"{ns}: dropped")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delete", action="store_true")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--pool", action="append", default=[])
    a = ap.parse_args()
    if a.delete and not a.yes:
        sys.exit("--delete needs --yes: run the dry run first and read its counts")
    r = get_redis()
    spaces = sorted(dec(c) for c in r.smembers("global:collections"))
    for ns in spaces + [f"global:pool:{p}" for p in a.pool]:
        scan(r, ns, a.delete)


if __name__ == "__main__":
    main()
