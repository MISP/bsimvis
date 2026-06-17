import os
import sys
import time
import requests
import uuid
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# Load environment variables
load_dotenv()

# Config
APP_HOST = os.getenv("APP_HOST", "localhost")
if APP_HOST == "0.0.0.0":
    APP_HOST = "localhost"
APP_PORT = os.getenv("APP_PORT", "5001")
BASE_URL = f"http://{APP_HOST}:{APP_PORT}"

# Paths
FILE_ARM = "./data/test/v01_arm_x64"
FILE_LINUX = "./data/test/v01_linux_x64"

MD5_ARM = "843841580c262ba277de71dc57336f70"
MD5_LINUX = "dbb7f163b18181227769d9769baf9407"

# Unique collections & pool to avoid collisions
run_id = uuid.uuid4().hex[:6]
SINGLE_COLL = f"single_coll_{run_id}"
SEP_COLL_ARM = f"sep_arm_{run_id}"
SEP_COLL_LINUX = f"sep_linux_{run_id}"
POOL_ID = f"pool_comp_{run_id}"


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def upload_file_async(executor, file_path, collection):
    def _upload():
        print(
            f"  Starting upload of {os.path.basename(file_path)} to collection '{collection}'..."
        )
        with open(file_path, "rb") as fh:
            raw = fh.read()
        params = {
            "collection": collection,
            "file_name": os.path.basename(file_path),
            "batch_name": "Test Run",
            "profile": "fast",
            "min_func_len": 10,
            "skip_sim": "false",
        }
        resp = requests.post(
            f"{BASE_URL}/api/file/upload",
            params=params,
            data=raw,
            headers={"Content-Type": "application/octet-stream"},
            timeout=60,
        )
        resp.raise_for_status()
        body = resp.json()
        pid = body.get("pipeline_id")
        print(
            f"  Upload of {os.path.basename(file_path)} to '{collection}' submitted: {pid}"
        )
        return pid

    return executor.submit(_upload)


def wait_for_pipelines(pipeline_ids):
    pipeline_ids = [pid for pid in pipeline_ids if pid]
    if not pipeline_ids:
        return
    print(f"    Waiting for pipelines {pipeline_ids}...", end="", flush=True)
    pending = set(pipeline_ids)
    while pending:
        time.sleep(2)
        done = []
        for pid in pending:
            try:
                resp = requests.get(f"{BASE_URL}/api/jobs/{pid}", timeout=10)
                resp.raise_for_status()
                status = resp.json().get("status", "unknown").lower()
                if status in ("completed", "failed", "cancelled"):
                    done.append(pid)
                    print(
                        f"\n    Pipeline {pid} → {status.upper()}", end="", flush=True
                    )
            except Exception:
                pass
        for pid in done:
            pending.remove(pid)
        if pending:
            print(".", end="", flush=True)
    print(" -> DONE")


def delete_collection_async(executor, collection):
    def _delete():
        print(f"  Requesting cleanup of collection {collection}...")
        try:
            resp = requests.post(
                f"{BASE_URL}/api/collection/delete",
                json={"collection": collection},
                timeout=10,
            )
            if resp.status_code == 200:
                return resp.json().get("job_id")
        except Exception as e:
            print(f"    Error deleting collection {collection}: {e}")
        return None

    return executor.submit(_delete)


def get_collection_binary_similarity(collection, md5_1, md5_2):
    print(f"  Fetching similarity for {collection} from DB...")
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()
    algo = "unweighted_cosine"
    score_key = f"{collection}:bin_sim:score:{algo}"

    # Format of member in collection bin_sim is {collection}:bin_sim:{algo}:{md5_1}::{md5_2}
    # ZSet stores keys in sorted order of md5. Ensure canonical ordering:
    m1, m2 = sorted([md5_1, md5_2])
    member_key = f"{collection}:bin_sim:{algo}:{m1}::{m2}"
    score = r.zscore(score_key, member_key)
    return score


def get_pool_binary_similarity(pool_id, coll_1, md5_1, coll_2, md5_2):
    print(f"  Fetching similarity for pool {pool_id} from DB...")
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()
    algo = "unweighted_cosine"
    score_key = f"global:pool:{pool_id}:bin_sim:score:{algo}"

    # Format: global:pool:{pool_id}:bin_sim:{algo}:{coll_a}:{md5_a}::{coll_b}:{md5_b}
    b1 = (coll_1, md5_1)
    b2 = (coll_2, md5_2)
    if b1 > b2:
        b1, b2 = b2, b1
    coll_a, md5_a = b1
    coll_b, md5_b = b2
    member_key = (
        f"global:pool:{pool_id}:bin_sim:{algo}:{coll_a}:{md5_a}::{coll_b}:{md5_b}"
    )
    score = r.zscore(score_key, member_key)
    return score


def main():
    section("1. Preparations & Cleanups")

    with ThreadPoolExecutor(max_workers=3) as executor:
        f1 = delete_collection_async(executor, SINGLE_COLL)
        f2 = delete_collection_async(executor, SEP_COLL_ARM)
        f3 = delete_collection_async(executor, SEP_COLL_LINUX)

        pids = [f1.result(), f2.result(), f3.result()]
        wait_for_pipelines(pids)

    from bsimvis.app.services.pool_service import pool_service

    pool_service.delete_pool(POOL_ID)

    try:
        # Check files exist
        assert os.path.exists(FILE_ARM), f"Missing file: {FILE_ARM}"
        assert os.path.exists(FILE_LINUX), f"Missing file: {FILE_LINUX}"

        # ==================================================================
        section("2. Parallel Ingestion (All Collections)")

        with ThreadPoolExecutor(max_workers=4) as executor:
            # Upload to SINGLE_COLL
            fu1 = upload_file_async(executor, FILE_ARM, SINGLE_COLL)
            fu2 = upload_file_async(executor, FILE_LINUX, SINGLE_COLL)
            # Upload to separate collections
            fu3 = upload_file_async(executor, FILE_ARM, SEP_COLL_ARM)
            fu4 = upload_file_async(executor, FILE_LINUX, SEP_COLL_LINUX)

            pids = [fu1.result(), fu2.result(), fu3.result(), fu4.result()]
            wait_for_pipelines(pids)

        # ==================================================================
        section("3. Single Collection Analysis")
        # Trigger build_sim for collection
        print("  Triggering function similarity build for collection...")
        resp = requests.post(
            f"{BASE_URL}/api/similarity/build",
            json={"collection": SINGLE_COLL, "all": True},
            timeout=10,
        )
        resp.raise_for_status()
        wait_for_pipelines([resp.json()["job_id"]])

        # Trigger function clustering
        print("  Triggering function clustering for collection...")
        resp = requests.post(
            f"{BASE_URL}/api/cluster/build",
            json={"collection": SINGLE_COLL},
            timeout=10,
        )
        resp.raise_for_status()
        wait_for_pipelines([resp.json()["job_id"]])

        # Trigger binary similarity build
        print("  Triggering binary similarity build for collection...")
        resp = requests.post(
            f"{BASE_URL}/api/bin_sim/build",
            json={"collection": SINGLE_COLL},
            timeout=10,
        )
        resp.raise_for_status()
        wait_for_pipelines([resp.json()["job_id"]])

        # Fetch score
        single_coll_score = get_collection_binary_similarity(
            SINGLE_COLL, MD5_ARM, MD5_LINUX
        )
        print(f"  => Score in SINGLE COLLECTION: {single_coll_score}")

        # Print single collection doc
        from bsimvis.app.services.redis_client import get_redis

        r = get_redis()
        m1, m2 = sorted([MD5_ARM, MD5_LINUX])
        single_doc_key = f"{SINGLE_COLL}:bin_sim:unweighted_cosine:{m1}::{m2}"
        single_doc = r.json().get(single_doc_key, "$")
        print(f"  [DEBUG] Single Collection Doc: {single_doc}")

        # ==================================================================
        section("4. Pool Analysis (Separated Collections)")
        # Create pool
        print("  Creating pool...")
        config = {
            "only_cross_collection": False,
            "func_sim_params": {
                "algo": "unweighted_cosine",
                "top_k": 1000,
                "min_score": 0.7,
            },
            "func_cluster_params": {
                "min_cluster_size": 2,
                "min_samples": 1,
                "epsilon": 0.001,
            },
            "file_sim_params": {
                "enabled": True,
                "algo": "unweighted_cosine",
                "top_k": 100,
                "min_score": 0.1,
                "min_cohesion": 0.5,
            },
            "file_cluster_params": {
                "enabled": True,
                "min_cluster_size": 2,
                "min_samples": 1,
                "epsilon": 0.001,
            },
        }
        success, msg = pool_service.create_pool(
            POOL_ID, "Comparison Pool", [SEP_COLL_ARM, SEP_COLL_LINUX], config
        )
        print(f"  Pool creation: {success} ({msg})")

        print("  Triggering pool similarity build...")
        resp = requests.post(f"{BASE_URL}/api/pool/{POOL_ID}/build", timeout=10)
        resp.raise_for_status()
        wait_for_pipelines([resp.json()["job_id"]])

        print("  Triggering pool clustering (and pool binary similarity)...")
        resp = requests.post(f"{BASE_URL}/api/pool/{POOL_ID}/cluster", timeout=10)
        resp.raise_for_status()
        wait_for_pipelines([resp.json()["job_id"]])

        # Fetch score
        pool_score = get_pool_binary_similarity(
            POOL_ID, SEP_COLL_ARM, MD5_ARM, SEP_COLL_LINUX, MD5_LINUX
        )
        print(f"  => Score in POOL: {pool_score}")

        # Print pool doc
        b1 = (SEP_COLL_ARM, MD5_ARM)
        b2 = (SEP_COLL_LINUX, MD5_LINUX)
        if b1 > b2:
            b1, b2 = b2, b1
        pool_doc_key = f"global:pool:{POOL_ID}:bin_sim:unweighted_cosine:{b1[0]}:{b1[1]}::{b2[0]}:{b2[1]}"
        pool_doc = r.json().get(pool_doc_key, "$")
        print(f"  [DEBUG] Pool Doc: {pool_doc}")

        # ==================================================================
        section("5. Comparison Results")
        print(f"  Single Collection Score: {single_coll_score}")
        print(f"  Pool-specific Score:     {pool_score}")

        if single_coll_score is not None and pool_score is not None:
            diff = abs(single_coll_score - pool_score)
            print(f"  Difference: {diff:.6f}")
            assert diff < 1e-5, f"Scores do not match! Diff: {diff}"
            print("\n  ✔ SUCCESS: The similarity scores match perfectly!")
        else:
            print("\n  ✗ FAILURE: One or both scores could not be resolved.")
            assert False, "Scores not resolved."

    finally:
        section("6. Final Cleanups")
        # with ThreadPoolExecutor(max_workers=3) as executor:
        #     f1 = delete_collection_async(executor, SINGLE_COLL)
        #     f2 = delete_collection_async(executor, SEP_COLL_ARM)
        #     f3 = delete_collection_async(executor, SEP_COLL_LINUX)
        #
        #     pids = [f1.result(), f2.result(), f3.result()]
        #     wait_for_pipelines(pids)
        #
        # pool_service.delete_pool(POOL_ID)
        pass


if __name__ == "__main__":
    main()
