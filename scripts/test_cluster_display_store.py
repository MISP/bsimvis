"""Precomputed cluster display hash == the SMEMBERS walk it replaces.

Run: uv run python -I scripts/test_cluster_display_store.py  (needs fakeredis)
"""

import fakeredis

from bsimvis.app.routes.search_file_clusters import _assign, _assign_stored
from bsimvis.app.services.cluster_display import clear_display, display_assignment

r = fakeredis.FakeStrictRedis()
col, algo = "c", "a"
tree = {
    "1": {"parent": None, "depth": 0, "n": 6, "coh": 0.3},
    "2": {"parent": "1", "depth": 1, "n": 3, "coh": 0.9},
}
for c, m in {"1": [f"f{i}" for i in range(1, 7)], "2": ["f1", "f2", "f3"]}.items():
    r.sadd(f"{col}:bin_cluster:{algo}:{c}:members", *m)

norm = lambda d: {k: set(v) for k, v in d.items()}
for cand in [{"f1", "f4", "f9"}, {f"f{i}" for i in range(1, 7)}]:
    a = _assign(r, col, algo, tree, cand)
    b = _assign_stored(r, col, algo, tree, "t1", cand)
    assert a[0] == b[0] and a[1] == b[1] and norm(a[2]) == norm(b[2]), (a, b)

from bsimvis.app.services.cluster_display import display_counts, display_direct

assert display_counts(r, col, algo, tree, "t1") == {"1": 3, "2": 3}
assert sorted(display_direct(r, col, algo, "t1", ["2"])["2"]) == ["f1", "f2", "f3"]

# a new tag (recluster) rebuilds and drops the old hash
r.sadd(f"{col}:bin_cluster:{algo}:2:members", "f9")
assert display_assignment(r, col, algo, tree, "t2")["f9"] == "2"
assert not r.exists(f"{col}:bin_cluster:display:{algo}:t1")
clear_display(r, col, algo)
assert not r.keys("*display*")
print("cluster display store ok")

# A uf:uuid hash left by an older union-find build must not hide the clusters
# of a later hierarchical build (it used to answer every lookup "not found").
import json as _json

from bsimvis.app.services import cluster_utils as _cu

r2 = fakeredis.FakeStrictRedis()
ns = "jaccard:library"
r2.hset(f"c:bin_cluster:{ns}:uf:uuid", mapping={"c:file:old": "deadbeef0000"})
r2.set(f"c:bin_cluster:{ns}:42:meta", _json.dumps({"cluster_uuid": "6a04573e5d6f"}))
r2.sadd(f"c:bin_cluster:list:{ns}", "42")
_cu._UUID_MAPS.clear()
assert (
    _cu.resolve_cluster_id_by_uuid(r2, "c", ns, "6a04573e5d6f", "bin_cluster") == "42"
)
# The stale entry's cid has no matching meta: still a miss, not a wrong cluster.
assert (
    _cu.resolve_cluster_id_by_uuid(r2, "c", ns, "deadbeef0000", "bin_cluster") is None
)
print("ok stale uf:uuid")
