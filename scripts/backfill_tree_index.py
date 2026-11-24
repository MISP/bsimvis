"""Build the per-node tree index (tree_parent / tree_children / uuid2cid) for
collections clustered before write_tree_links existed. Reads the stored
tree_links blob and leaves it untouched.

    uv run python scripts/backfill_tree_index.py COLLECTION [--prefix bin_cluster] [--algo A]
"""

import argparse
import json
import time

from bsimvis.app.services.cluster_utils import (
    _tree_keys,
    rebuild_tree_index,
    resolve_cluster_id_by_uuid,
)
from bsimvis.app.services.collection_config import resolve_collection_algo
from bsimvis.app.services.redis_client import get_redis

ap = argparse.ArgumentParser()
ap.add_argument("collection")
ap.add_argument("--prefix", default="cluster", choices=["cluster", "bin_cluster"])
ap.add_argument("--algo", help="default: the collection's configured algo")
ap.add_argument("--no-uuid", action="store_true", help="skip warming uuid2cid")
a = ap.parse_args()

r = get_redis()
algo = a.algo or resolve_collection_algo(a.collection, None)
blob_k = _tree_keys(a.collection, a.prefix, algo)[0]
raw = r.get(blob_k)
if not raw:
    raise SystemExit(f"no blob at {blob_k}")
t = time.perf_counter()
n = rebuild_tree_index(r, a.collection, a.prefix, algo, json.loads(raw))
print(f"{blob_k}: indexed {n} edges in {time.perf_counter() - t:.1f}s")
if not a.no_uuid:
    t = time.perf_counter()
    resolve_cluster_id_by_uuid(r, a.collection, algo, "warmup", a.prefix)
    k = _tree_keys(a.collection, a.prefix, algo)[3]
    print(f"uuid2cid: {r.hlen(k)} uuids in {time.perf_counter() - t:.1f}s")
