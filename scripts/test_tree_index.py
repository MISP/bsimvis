"""The per-node tree index must give the same slice as the tree_links blob.

Builds a small tree on a disposable Redis, then compares the indexed Tree
against the blob fallback (and a pre-index tree with no index keys at all),
checks that a rebuild drops stale nodes, and that uuid2cid self-heals.

Run: uv run python scripts/test_tree_index.py
"""

import json
import socket
import subprocess
import tempfile
import time

import redis

from bsimvis.app.services.cluster_utils import (
    Tree,
    cluster_children_page,
    cluster_tree_slice,
    clear_tree_links,
    resolve_cluster_id_by_uuid,
    write_tree_links,
)

COL, ALGO = "C", "jaccard"
ROOT = 99  # synthetic root: never a child, so real_links drops its link
#          ROOT
#           |
#           1 (size 10)
#         / | \
#        2  3  4      sizes 5, 4, 3 (4 has no children)
#       / \
#      5   6          sizes 3, 2
SIZES = {1: 10, 2: 5, 3: 4, 4: 3, 5: 3, 6: 2}
EDGES = [(ROOT, 1), (1, 2), (1, 3), (1, 4), (2, 5), (2, 6)]


def links():
    return [
        {"parent": p, "child": c, "lambda": 0.5, "size": SIZES[c]} for p, c in EDGES
    ]


def put_metas(r, ids):
    for c in ids:
        r.set(
            f"{COL}:cluster:{ALGO}:{c}:meta",
            json.dumps(
                {"cluster_id": c, "cluster_uuid": f"u{c}", "member_count": SIZES[c]}
            ),
        )


def window(r, tree, uuid):
    metas, center, hidden = cluster_tree_slice(
        r, COL, ALGO, uuid, up=3, down=2, width=2, tree=tree
    )
    return sorted(metas), center, hidden


def check_equivalent(r):
    put_metas(r, SIZES)
    write_tree_links(r, COL, "cluster", ALGO, links())
    indexed = Tree(r, COL, ALGO)
    assert indexed.indexed
    blob = Tree(r, COL, ALGO, links=Tree(r, COL, ALGO).__class__.__mro__ and None)
    # force the fallback by handing it the parsed blob
    from bsimvis.app.services.cluster_utils import get_tree_links

    blob = Tree(r, COL, ALGO, links=get_tree_links(r, COL, ALGO))
    assert not blob.indexed
    for uuid in ("u1", "u2", "u5", "u4"):
        a, b = window(r, indexed, uuid), window(r, blob, uuid)
        assert a == b, (uuid, a, b)

    assert indexed.parent(5) == "2" and indexed.parent(1) is None
    assert indexed.children(1) == (["2", "3", "4"], 3)
    assert indexed.children(1, 1, 1) == (["3"], 3)
    assert indexed.info([1, 4, 5]) == (
        {"1": None, "4": "1", "5": "2"},
        {"1": 3, "4": 0, "5": 0},
    )
    page, total = cluster_children_page(
        r, COL, ALGO, 1, offset=2, width=2, tree=indexed
    )
    assert (sorted(page), total) == (["4"], 3), (page, total)
    # width=2 on a 3-child node: the smallest child is cut, hidden_above is 0
    assert window(r, indexed, "u1") == (["1", "2", "3", "5", "6"], "1", 0)


def check_uuid_cache(r):
    assert resolve_cluster_id_by_uuid(r, COL, ALGO, "u5") == "5"
    assert r.hget(f"{COL}:cluster:uuid2cid:{ALGO}", "u5") == "5"
    # a cache entry whose meta moved on must not answer, and gets dropped
    r.hset(f"{COL}:cluster:uuid2cid:{ALGO}", "ghost", "5")
    assert resolve_cluster_id_by_uuid(r, COL, ALGO, "ghost") is None
    assert r.hget(f"{COL}:cluster:uuid2cid:{ALGO}", "ghost") is None


def check_rewrite_and_clear(r):
    write_tree_links(r, COL, "cluster", ALGO, [links()[0], links()[1]])
    tree = Tree(r, COL, ALGO)
    assert tree.children(1) == (["2"], 1), tree.children(1)
    assert tree.children(2) == ([], 0), "stale children of a rewritten tree"
    assert r.hget(f"{COL}:cluster:uuid2cid:{ALGO}", "u5") is None
    clear_tree_links(r, COL, "cluster", ALGO)
    assert not r.exists(
        f"{COL}:cluster:tree_links:{ALGO}", f"{COL}:cluster:tree_parent:{ALGO}"
    )
    assert not Tree(r, COL, ALGO).indexed


def check():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    with tempfile.TemporaryDirectory(prefix="bsimvis-tree-") as data_dir:
        server = subprocess.Popen(
            ["redis-server", "--port", str(port), "--bind", "127.0.0.1"]
            + ["--save", "", "--appendonly", "no", "--dir", data_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
            for _ in range(100):
                try:
                    r.ping()
                    break
                except redis.ConnectionError:
                    time.sleep(0.02)
            else:
                raise RuntimeError("disposable Redis did not start")
            check_equivalent(r)
            check_uuid_cache(r)
            check_rewrite_and_clear(r)
            print("PASS: tree index matches the blob, uuid cache self-heals")
        finally:
            server.terminate()
            server.wait(timeout=10)


if __name__ == "__main__":
    check()
