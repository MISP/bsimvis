"""Run tiny real builders against a disposable Redis instance; never uses project DBs."""

import json
import socket
import subprocess
import tempfile
import time
from unittest.mock import patch

import redis

from bsimvis.app.services import bin_cluster_service as bin_cluster_service_module
from bsimvis.app.services.bin_cluster_service import BinClusterService
from bsimvis.app.services.bin_sim_service import BinSimService
from bsimvis.app.services.cluster_service import ClusterService
from bsimvis.app.services import collection_config, pool_service as pool_service_module
from bsimvis.app.services.collection_config import set_collection_param
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.pool_service import PoolService
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.similarity.registry import names

CONFIG = {
    "similarity.algo": "unweighted_cosine",  # Deliberately conflicts with each collection.
    "similarity.top_k": 100,
    "similarity.min_score": 0.1,
    "similarity.min_features": 1,
    "similarity.discovery": False,
    "similarity.unweighted_match": False,
    "clustering.engine": "threshold_uf",
    "clustering.bin_engine": "threshold_uf",
    "clustering.uf_threshold": 0.98,
    "clustering.bin_uf_threshold": 0.0,
    "clustering.min_cluster_size": 2,
    "clustering.min_samples": 1,
    "clustering.min_sim": 0.0,
    "clustering.min_cohesion": 0.0,
    "clustering.min_features": 0,
}
FUNCTION_ENGINES = ("threshold_uf", "hierarchical_uf", "hdbscan")
BINARY_ENGINES = ("threshold_uf", "hierarchical_uf", "hierarchical_snn")


def seed_collection(r, collection):
    for md5, addr in (("a001", "00000100"), ("b002", "00000200")):
        fid = f"{collection}:func:{md5}:{addr}"
        r.sadd(f"{collection}:indexed:functions", fid)
        r.sadd(f"{collection}:batch:fixture:functions", fid)
        r.sadd(f"{collection}:all_functions", fid)
        r.sadd(f"{collection}:idx:file:functions:{md5}", fid)
        r.sadd(f"{collection}:all_files", f"{collection}:file:{md5}")
        r.zadd(f"{collection}:idx:func:bsim_features_count", {fid: 12})
        r.set(
            f"{fid}:meta",
            json.dumps(
                {
                    "file_md5": md5,
                    "entrypoint_address": addr,
                    "function_name": "fixture",
                    "bsim_features_count": 12,
                    "tags": [],
                }
            ),
        )
        r.set(
            f"{collection}:file:{md5}:meta",
            json.dumps(
                {
                    "file_md5": md5,
                    "file_name": f"{md5}.bin",
                    "tags": [],
                }
            ),
        )
        vector = {f"{i:x}": 1 for i in range(12)}
        r.zadd(f"{fid}:vec:tf", vector)
        r.set(f"{fid}:vec:norm", str(12**0.5))
        for feature in vector:
            r.zadd(f"{collection}:feature:{feature}:functions", {fid: 1})
    return "a001", "b002"


def assert_pair(r, key, algo):
    members = r.zrange(key, 0, -1)
    assert members, f"no generated scores in {key}"
    docs = [json.loads(r.get(sid)) for sid in members]
    assert all(doc.get("algo") == algo for doc in docs), (key, docs)
    return members


def run_matrix(r):
    similarity = SimilarityService(r)
    file_sim = BinSimService(r)
    function_cluster = ClusterService(r)
    binary_cluster = BinClusterService(r)
    bin_cluster_service_module.bin_cluster_service.r = r
    algos = names(buildable=True, milvus_enabled=False)

    for algo in algos:
        collection = f"matrix_{algo}"
        md5_a, md5_b = seed_collection(r, collection)
        set_collection_param(collection, "algo", algo)

        # Every function algorithm must persist under its selected collection namespace,
        # even when the call carries a conflicting global/requested algorithm.
        assert similarity.build_batch(
            collection,
            batch_uuid="fixture",
            algo="unweighted_cosine",
            min_score=0.1,
            min_features=1,
            index_depth="none",
        )
        _fid = f"{collection}:func:a001:00000100"
        _raw = r.zrange(f"{_fid}:vec:tf", 0, -1, withscores=True)
        _args = similarity.build_discovery_args(
            _fid, collection, algo, 0.1, 100, 1, _raw
        )[1]
        sim_ids = assert_pair(r, f"{collection}:sim:score:{algo}", algo)
        assert all(r.exists(sid) for sid in sim_ids)
        assert (
            not r.exists(f"{collection}:sim:score:unweighted_cosine")
            if algo != "unweighted_cosine"
            else True
        )

        # All function clustering engines consume the generated algorithm namespace.
        for engine in FUNCTION_ENGINES:
            assert function_cluster.run_clustering(collection, algo=algo, engine=engine)
            assert r.smembers(f"{collection}:cluster:list:{algo}"), (algo, engine)

        # The same selected algorithm tags file-sim docs; each binary engine must
        # read that algorithm namespace and write its own expected cluster namespace.
        assert file_sim.build_bin_sim(collection, algo=algo, md5_a=md5_a, md5_b=md5_b)
        bin_key = f"{collection}:bin_sim:score:{algo}"
        bin_ids = assert_pair(r, bin_key, algo)
        assert all(r.exists(sid) for sid in bin_ids)
        for engine in BINARY_ENGINES:
            assert binary_cluster.run_clustering(collection, algo=algo, engine=engine)
            suffix = ":snn" if engine == "hierarchical_snn" else ""
            assert r.smembers(f"{collection}:bin_cluster:list:{algo}{suffix}"), (
                algo,
                engine,
            )

        pool_id = f"pool_{algo}"
        members = (f"{collection}_left", f"{collection}_right")
        for member in members:
            seed_collection(r, member)
        pool_service = PoolService(r)
        pool_service.create_pool(
            pool_id,
            pool_id,
            members,
            {
                "func_sim_params": {"algo": algo, "min_score": 0.1, "min_features": 1},
                "func_cluster_params": {"cluster_algo": FUNCTION_ENGINES[0]},
                "file_cluster_params": {"cluster_algo": BINARY_ENGINES[0]},
            },
        )
        pool_service_module.pool_service.r = r
        assert similarity.build_pool(pool_id)
        pool_ns = f"global:pool:{pool_id}"
        pool_sim_ids = assert_pair(r, f"{pool_ns}:sim:score", algo)
        assert all(r.exists(sid) for sid in pool_sim_ids)
        for engine in FUNCTION_ENGINES:
            pool_service.update_pool_config(
                pool_id, {"func_cluster_params": {"cluster_algo": engine}}
            )
            assert function_cluster.clear_clustering(pool_ns, algo)
            assert function_cluster.run_pool_clustering(pool_id)
            assert r.smembers(f"{pool_ns}:cluster:list:{algo}"), ("pool", algo, engine)
        assert similarity.build_pool_bin_sim(pool_id)
        pool_bin_key = f"{pool_ns}:bin_sim:score:{algo}"
        pool_bin_ids = r.zrange(pool_bin_key, 0, -1)
        assert pool_bin_ids, f"no pool file similarity scores in {pool_bin_key}"
        for sid in pool_bin_ids:
            assert json.loads(r.get(sid)).get("algo") == algo
        for engine in BINARY_ENGINES:
            pool_service.update_pool_config(
                pool_id, {"file_cluster_params": {"cluster_algo": engine}}
            )
            CONFIG["clustering.bin_engine"] = engine
            for axis in ("code", "library", "content"):
                assert binary_cluster.clear_clusters(pool_ns, algo, axis=axis)
            assert function_cluster.run_pool_bin_clustering(pool_id)
            assert r.smembers(
                f"{pool_ns}:bin_cluster:list:{algo}:code"
                + (":snn" if engine == "hierarchical_snn" else "")
            ), ("pool", algo, engine)
        pool_bin_sid = pool_bin_ids[0]
        assert pool_service.clear_pool_targets(pool_id, ["binary_similarity"]) is None
        assert not r.exists(pool_bin_sid)
        assert not r.zcard(pool_bin_key)
        assert r.exists(bin_ids[0]), "pool cleanup crossed into its member collection"

    # A collection can retain records under older algo namespaces after config changes.
    # Clearing one algorithm must preserve the other algorithm's docs, scores and indexes.
    shared = "matrix_maintenance"
    md5_a, md5_b = seed_collection(r, shared)
    for algo in algos[:2]:
        set_collection_param(shared, "algo", algo)
        assert similarity.build_batch(
            shared,
            batch_uuid="fixture",
            algo="unweighted_cosine",
            min_score=0.1,
            min_features=1,
            index_depth="none",
        )
        assert file_sim.build_bin_sim(shared, algo=algo, md5_a=md5_a, md5_b=md5_b)
    old_algo, active_algo = algos[:2]
    old_bin_sid = r.zrange(f"{shared}:bin_sim:score:{old_algo}", 0, -1)[0]
    active_bin_sid = r.zrange(f"{shared}:bin_sim:score:{active_algo}", 0, -1)[0]
    assert file_sim.clear_bin_sim(shared, algo=old_algo)
    assert not r.exists(old_bin_sid)
    assert not r.zcard(f"{shared}:bin_sim:score:{old_algo}")
    assert r.exists(active_bin_sid)
    assert r.zscore(f"{shared}:bin_sim:score:{active_algo}", active_bin_sid) is not None
    assert r.sismember(f"{shared}:all_bin_sims", active_bin_sid)
    algo_bucket = f"{shared}:idx:bin_sim:algo:{active_algo}"
    assert r.sismember(algo_bucket, active_bin_sid)

    old_sim_sid = r.zrange(f"{shared}:sim:score:{old_algo}", 0, -1)[0]
    active_sim_sid = r.zrange(f"{shared}:sim:score:{active_algo}", 0, -1)[0]
    assert similarity.clear_all(shared, algo=old_algo)
    assert not r.exists(old_sim_sid)
    assert r.exists(active_sim_sid)
    assert r.zscore(f"{shared}:sim:score:{active_algo}", active_sim_sid) is not None

    return algos


def check():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    with tempfile.TemporaryDirectory(prefix="bsimvis-matrix-") as data_dir:
        server = subprocess.Popen(
            [
                "redis-server",
                "--port",
                str(port),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                data_dir,
            ],
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
            with (
                patch.object(
                    config_service,
                    "get",
                    side_effect=lambda key, default=None: CONFIG.get(key, default),
                ),
                patch.object(collection_config, "get_redis", return_value=r),
            ):
                algos = run_matrix(r)
            print(
                f"PASS: {len(algos)} algorithms x {len(FUNCTION_ENGINES)} function engines x {len(BINARY_ENGINES)} binary engines"
            )
        finally:
            server.terminate()
            server.wait(timeout=10)


if __name__ == "__main__":
    check()
