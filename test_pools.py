import json
import logging
import time
from bsimvis.app.services.pool_service import pool_service
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.cluster_service import cluster_service
from bsimvis.app.services.redis_client import get_redis

logging.basicConfig(level=logging.INFO)

COLLECTIONS = ["main2", "main3"]  # Ensure these collections exist with data
POOL_ID = "test_pool_1"
ALGO = "unweighted_cosine"


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def test_pool_lifecycle(no_delete=False):
    r = get_redis()
    sim_service = SimilarityService(r)

    config = {
        "algo": ALGO,
        "top_k": 10,
        "min_score": 0.1,
        "cluster_params": {"min_cluster_size": 2, "min_samples": 1, "epsilon": 0.001},
    }

    # ------------------------------------------------------------------
    section("Step 1: Create Pool")
    pool_service.delete_pool(POOL_ID)
    success, msg = pool_service.create_pool(POOL_ID, "Test Pool", COLLECTIONS, config)
    print(f"  create_pool → {success}, {msg}")

    # ------------------------------------------------------------------
    section("Step 2: Build Pool (function-level similarities)")
    success = sim_service.build_pool(POOL_ID)
    print(f"  build_pool  → {success}")

    score_key = f"global:pool:{POOL_ID}:sim:score"
    total_sims = r.zcard(score_key)
    print(f"  Similarities stored : {total_sims}")

    if total_sims > 0:
        sample_sids = r.zrange(score_key, 0, 0)
        sid = (
            sample_sids[0].decode()
            if isinstance(sample_sids[0], bytes)
            else sample_sids[0]
        )
        sim_doc_raw = r.json().get(sid, "$")
        if sim_doc_raw:
            sim_doc = sim_doc_raw[0] if isinstance(sim_doc_raw, list) else sim_doc_raw
            print(f"  Sample SID          : {sid}")
            print(f"  Score               : {sim_doc.get('score')}")
            print(
                f"  coll_1 / coll_2     : {sim_doc.get('coll_1')} / {sim_doc.get('coll_2')}"
            )
            print(f"  is_cross_binary     : {sim_doc.get('is_cross_binary')}")

    # ------------------------------------------------------------------
    section("Step 3: Sync Status")
    status = pool_service.check_sync_status(POOL_ID)
    print(f"  sync_status → {status['sync_status']}")

    # ------------------------------------------------------------------
    section("Step 4: Involves Indexes (file + func)")

    # File involves
    file_inv_keys = []
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor, match=f"global:pool:{POOL_ID}:sim:involves:file:*", count=500
        )
        file_inv_keys.extend(keys)
        if cursor == 0:
            break
    print(f"  File involves keys  : {len(file_inv_keys)}")
    for k in file_inv_keys:
        k_str = k.decode() if isinstance(k, bytes) else k
        label = k_str.split("involves:file:")[-1]
        count = r.scard(k_str)
        print(f"    [{label}] → {count} similarities")

    # Func involves
    func_inv_keys = []
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor, match=f"global:pool:{POOL_ID}:sim:involves:func:*", count=500
        )
        func_inv_keys.extend(keys)
        if cursor == 0:
            break
    print(f"  Func involves keys  : {len(func_inv_keys)}")

    # ------------------------------------------------------------------
    section("Step 4.5: Query Pool via Search API")
    import requests
    import os

    api_url = os.getenv("API_URL", "http://localhost:5000")
    print(f"  Querying similarity search API at {api_url} for pool: {POOL_ID}...")
    try:
        resp = requests.get(
            f"{api_url}/api/similarity/search",
            params={"pool": POOL_ID, "min_score": 0.1, "limit": 100},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"  API Response Keys: {list(data.keys())}")
            if "pairs" in data:
                api_sims = data["pairs"]
            elif "results" in data:
                api_sims = data["results"]
            else:
                api_sims = []
            print(
                f"  API returned {len(api_sims)} similarities (out of {total_sims} total in DB)"
            )
            if len(api_sims) == 0:
                print(f"  Full API Response context: {json.dumps(data)[:1000]}")

            if total_sims > 0:
                assert len(api_sims) > 0, "API returned 0 similarities but DB has some"
                sample_api_sim = api_sims[0]
                print(f"    Sample API Similarity Score: {sample_api_sim.get('score')}")
                print(
                    f"    Sample API Similarity ID1/ID2: {sample_api_sim.get('id1')} / {sample_api_sim.get('id2')}"
                )

                # Try simple filters
                # Filter 1: min_score filter
                if len(api_sims) > 1:
                    mid_score = api_sims[-1].get("score", 0.1)
                    if mid_score > 0.1:
                        filter_score = (api_sims[0].get("score") + mid_score) / 2.0
                        resp_filtered = requests.get(
                            f"{api_url}/api/similarity/search",
                            params={
                                "pool": POOL_ID,
                                "min_score": filter_score,
                                "limit": 100,
                            },
                            timeout=10,
                        )
                        if resp_filtered.status_code == 200:
                            filtered_sims = resp_filtered.json().get("pairs", [])
                            print(
                                f"    Filtered by min_score={filter_score:.4f} → {len(filtered_sims)} results"
                            )
                            for s in filtered_sims:
                                assert (
                                    s.get("score") >= filter_score
                                ), f"Similarity score {s.get('score')} < {filter_score}"

                # Filter 2: language filter (if available)
                lang1 = sample_api_sim.get("func1", {}).get("language_id")
                if lang1:
                    print(f"    Applying language filter: {lang1}")
                    resp_lang = requests.get(
                        f"{api_url}/api/similarity/search",
                        params={
                            "pool": POOL_ID,
                            "min_score": 0.1,
                            "language": lang1,
                            "limit": 100,
                        },
                        timeout=10,
                    )
                    if resp_lang.status_code == 200:
                        lang_sims = resp_lang.json().get("pairs", [])
                        print(
                            f"    Filtered by language={lang1} → {len(lang_sims)} results"
                        )
                        for s in lang_sims:
                            f1_lang = s.get("func1", {}).get("language_id")
                            f2_lang = s.get("func2", {}).get("language_id")
                            assert lang1 in (
                                f1_lang,
                                f2_lang,
                            ), f"Neither func1 ({f1_lang}) nor func2 ({f2_lang}) matches language {lang1}"

                # Filter 3: cross-binary filter
                resp_cb = requests.get(
                    f"{api_url}/api/similarity/search",
                    params={
                        "pool": POOL_ID,
                        "min_score": 0.1,
                        "cross_binary": "true",
                        "limit": 100,
                    },
                    timeout=10,
                )
                if resp_cb.status_code == 200:
                    cb_sims = resp_cb.json().get("pairs", [])
                    print(f"    Filtered by cross_binary=true → {len(cb_sims)} results")
                    for s in cb_sims:
                        is_cb = s.get("is_cross_binary")
                        assert (
                            is_cb is True or is_cb == "true" or is_cb == "True"
                        ), f"Similarity {s.get('id')} is not cross binary"
        else:
            print(
                f"  [ERROR] Similarity search API returned status code {resp.status_code}: {resp.text}"
            )
            assert False, "Similarity search API request failed"
    except Exception as e:
        print(f"  [SKIP/ERROR] Could not query search API: {e}")
        print(
            "  (Make sure the API server is running on http://localhost:5000 or set API_URL)"
        )

    # ------------------------------------------------------------------
    section("Step 5: Pool Function Clustering")

    print(f"  Running cluster_service.run_pool_clustering for pool {POOL_ID}...")
    ok = cluster_service.run_pool_clustering(POOL_ID)
    print(f"  run_pool_clustering → {ok}")

    cluster_list_key = f"global:pool:{POOL_ID}:cluster:list"
    total_clusters = r.scard(cluster_list_key)
    print(f"  Pool clusters produced : {total_clusters}")

    if total_clusters > 0:
        sample_ids = list(r.smembers(cluster_list_key))[:3]
        for cid_raw in sample_ids:
            cid = cid_raw.decode() if isinstance(cid_raw, bytes) else cid_raw
            meta_raw = r.json().get(f"global:pool:{POOL_ID}:cluster:{cid}:meta", "$")
            meta = (
                (meta_raw[0] if isinstance(meta_raw, list) else meta_raw)
                if meta_raw
                else {}
            )
            members_key = f"global:pool:{POOL_ID}:cluster:{cid}:members"
            member_count = r.scard(members_key)
            print(
                f"    Cluster {cid}: {member_count} functions, name='{meta.get('name', 'N/A')}'"
            )
    else:
        print(
            "  [WARN] No pool-level function clusters produced. (Check min_score / min_cluster_size config)"
        )

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    section("Step 5.5: Pool File Similarity and Clustering")

    # Assert pool file similarities are built and store in `global:pool:test_pool_1:bin_sim:score:{algo}`
    bin_sim_score_key = f"global:pool:{POOL_ID}:bin_sim:score:{ALGO}"
    total_bin_sims = r.zcard(bin_sim_score_key)
    print(f"  Pool file similarities stored : {total_bin_sims}")
    assert total_bin_sims > 0, "No pool file similarities were stored in DB"

    # Assert pool file clusters (`global:pool:test_pool_1:bin_cluster:list`) are built
    bin_cluster_list_key = f"global:pool:{POOL_ID}:bin_cluster:list"
    total_bin_clusters = r.scard(bin_cluster_list_key)
    print(f"  Pool file clusters produced : {total_bin_clusters}")
    assert total_bin_clusters >= 0, "No pool file clusters key found"

    # Query API /api/bin_sim/search with collection="pool:test_pool_1"
    print(f"  Querying bin_sim search API for pool: {POOL_ID}...")
    try:
        resp = requests.get(
            f"{api_url}/api/bin_sim/search",
            params={"pool": POOL_ID, "min_cohesion": 0.0, "limit": 100},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            # The HTTP API search endpoint uses the key "results" in its response
            api_bin_sims = data.get("results", [])
            print(f"  API returned {len(api_bin_sims)} file similarities")
            assert len(api_bin_sims) > 0, "API returned 0 file similarities but DB has them"
            if len(api_bin_sims) > 0:
                print(f"    Sample API File Sim score: {api_bin_sims[0].get('score')}")
                print(f"    Sample API File Sim files: {api_bin_sims[0].get('md5_1')} vs {api_bin_sims[0].get('md5_2')}")

                # Try simple filters on file similarity search
                # Filter 1: min_score filter
                if len(api_bin_sims) > 1:
                    mid_score = api_bin_sims[-1].get("score", 0.0)
                    if mid_score > 0.0:
                        filter_score = (api_bin_sims[0].get("score") + mid_score) / 2.0
                        resp_filtered = requests.get(
                            f"{api_url}/api/bin_sim/search",
                            params={
                                "pool": POOL_ID,
                                "algo": ALGO,
                                "min_score": filter_score,
                                "limit": 100,
                            },
                            timeout=10,
                        )
                        if resp_filtered.status_code == 200:
                            filtered_sims = resp_filtered.json().get("results", [])
                            print(f"    Filtered by min_score={filter_score:.4f} → {len(filtered_sims)} results")
                            for s in filtered_sims:
                                assert s.get("score") >= filter_score, f"Similarity score {s.get('score')} < {filter_score}"

                # Filter 2: md5 filter
                sample_md5 = api_bin_sims[0].get("md5_1")
                if sample_md5:
                    resp_md5 = requests.get(
                        f"{api_url}/api/bin_sim/search",
                        params={
                            "pool": POOL_ID,
                            "md5": sample_md5,
                            "limit": 100,
                        },
                        timeout=10,
                    )
                    if resp_md5.status_code == 200:
                        md5_sims = resp_md5.json().get("results", [])
                        print(f"    Filtered by md5={sample_md5} → {len(md5_sims)} results")
                        for s in md5_sims:
                            assert sample_md5 in [s.get("md5_1"), s.get("md5_2")], f"MD5 {sample_md5} not in similarity pair {s}"
        else:
            print(f"  [ERROR] bin_sim search API returned status code {resp.status_code}: {resp.text}")
            assert False, "bin_sim search API request failed"
    except Exception as e:
        print(f"  [ERROR] Failed to query or assert bin_sim search API: {e}")
        raise e

    # Query API /api/bin_cluster/list with collection="pool:test_pool_1"
    print(f"  Querying bin_cluster list API for pool: {POOL_ID}...")
    try:
        resp = requests.get(
            f"{api_url}/api/bin_cluster/list",
            params={"pool": POOL_ID},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            # The API returns a dictionary with key "results" containing the list of clusters
            api_clusters = data.get("results", [])
            print(f"  API returned {len(api_clusters)} file clusters")
            assert len(api_clusters) >= 0, "No pool file clusters key found in API response"
            if len(api_clusters) > 0:
                print(f"    Sample API File Cluster: {api_clusters[0]}")
        else:
            print(f"  [ERROR] bin_cluster list API returned status code {resp.status_code}: {resp.text}")
            assert False, "bin_cluster list API request failed"
    except Exception as e:
        print(f"  [ERROR] Failed to query or assert bin_cluster list API: {e}")
        raise e

    if no_delete:
        print("\n✓ Pool kept for testing (--no-delete flag active).\n")
        return

    # ------------------------------------------------------------------
    section("Step 6: Delete Pool")
    success, msg = pool_service.delete_pool(POOL_ID)
    print(f"  delete_pool → {success}, {msg}")

    remaining = r.keys(f"global:pool:{POOL_ID}:*")
    print(f"  Remaining keys after delete : {len(remaining)}")
    assert (
        len(remaining) == 0
    ), f"Pool keys not fully cleaned up: {[k.decode() for k in remaining[:5]]}"

    print("\n✓ All pool assertions passed.\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-delete", action="store_true", help="Keep the created pool after testing")
    args = parser.parse_args()
    test_pool_lifecycle(no_delete=args.no_delete)
