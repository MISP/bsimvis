"""Regression: tiny exact matches remain searchable, immediately and after backfill.

Run: uv run python scripts/test_small_similarity_indexes.py
Uses a temporary Kvrocks process and data directory; never touches the main stack.
"""

import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from unittest.mock import patch

import redis
from flask import Flask

from bsimvis.app.routes.search_similarity import similarity_search
from bsimvis.app.services.lua_manager import lua_manager
from bsimvis.app.services.similarity_service import SimilarityService
from backfill_small_similarity_indexes import backfill


def check(r):
    service = SimilarityService(r)
    app = Flask(__name__)
    search = r.register_script(
        Path("bsimvis/app/lua/search_similarity.lua").read_text()
    )
    for pool in [None, "test_pool"]:
        namespace = f"global:pool:{pool}" if pool else "test"
        fids = ["test:func:aaa:00000100", "test:func:bbb:00000200"]
        for fid in fids:
            md5, address = fid.split(":")[-2:]
            r.set(
                fid + ":meta",
                json.dumps(
                    {
                        "function_name": "tiny_resolver",
                        "entrypoint_address": address,
                        "file_md5": md5,
                        "tags": ["library"],
                        "bsim_features_count": 3,
                    }
                ),
            )
            r.set(
                f"test:file:{md5}:meta",
                json.dumps(
                    {
                        "file_md5": md5,
                        "file_name": "fixture.elf",
                        "tags": ["linux"],
                    }
                ),
            )
            r.set(fid + ":funcid", "exact_hash")
            r.sadd("test:funcid:exact_hash", fid)
            r.zadd(fid + ":vec:tf", {"a": 1, "b": 1, "c": 1})
        for depth in ["none", "minimal", "full"]:
            # Only test indexes are removed; scores and source metadata stay intact.
            for key in list(r.scan_iter(f"{namespace}:idx:sim:*")):
                r.delete(key)
            service._hash_match_small(
                "test", "unweighted_cosine", fids[:1], depth, pool_id=pool
            )
            prefix = namespace + ":sim:" + ("" if pool else "unweighted_cosine:")
            sids = list(r.scan_iter(prefix + "*::*"))
            assert len(sids) == 1
            sid = sids[0]
            assert json.loads(r.get(sid))["score"] == 1
            md5_key = f"{namespace}:idx:sim:file_md5:aaa"
            name_key = f"{namespace}:idx:sim:function_name:tiny_resolver"
            assert r.sismember(md5_key, sid) == (depth != "none")
            assert r.sismember(name_key, sid) == (depth == "full")
            before = r.get(sid)
            assert (
                backfill(r, namespace, "unweighted_cosine", apply=False, pool_id=pool)
                == 1
            )
            assert r.sismember(name_key, sid) == (depth == "full")
            for _ in range(2):
                assert (
                    backfill(
                        r, namespace, "unweighted_cosine", apply=True, pool_id=pool
                    )
                    == 1
                )
                assert r.sismember(md5_key, sid)
                assert r.sismember(name_key, sid)
                assert r.sismember(f"{namespace}:idx:sim:file_name:fixture.elf", sid)
                assert r.get(sid) == before
            if not pool:
                query = (
                    "/api/similarity/search?collection=test&md5=aaa"
                    "&address=00000100&name=tiny_resolver&file_name=fixture.elf"
                    "&min_score=.9&min_features=0"
                )
                with (
                    app.test_request_context(query),
                    patch(
                        "bsimvis.app.routes.search_similarity.get_redis", return_value=r
                    ),
                    patch.object(lua_manager, "get_script", return_value=search),
                ):
                    result = similarity_search()
                assert isinstance(result, dict), result
                assert result["total"] == 1, result
                assert result["pairs"][0]["score"] == 1


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="small-sim-test-") as directory:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        config = Path(directory) / "kvrocks.conf"
        config.write_text(f"bind 127.0.0.1\nport {port}\ndir {directory}\n")
        process = subprocess.Popen(
            [
                str(Path("bin/kvrocks").resolve()),
                "-c",
                str(config),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
            for _ in range(100):
                assert process.poll() is None, "Temporary Kvrocks failed to start"
                try:
                    if r.ping():
                        break
                except redis.ConnectionError:
                    time.sleep(0.05)
            check(r)
            print(
                "PASS: tiny-match indexing, complex search, pool backfill, idempotence"
            )
        finally:
            process.terminate()
            process.wait(timeout=5)
