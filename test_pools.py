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
            json={
                "collection": SINGLE_COLL,
                "all": True,
                "algo": "unweighted_cosine",
                "top_k": 1000,
                # min_score omitted: use config default so collection & pool stay aligned
            },
            timeout=10,
        )
        resp.raise_for_status()
        wait_for_pipelines([resp.json()["job_id"]])

        # Trigger function clustering
        print("  Triggering function clustering for collection...")
        resp = requests.post(
            f"{BASE_URL}/api/cluster/build",
            json={
                "collection": SINGLE_COLL,
                # params omitted: use config defaults so collection & pool stay aligned
            },
            timeout=10,
        )
        resp.raise_for_status()
        wait_for_pipelines([resp.json()["job_id"]])

        # Trigger binary similarity build
        print("  Triggering binary similarity build for collection...")
        resp = requests.post(
            f"{BASE_URL}/api/bin_sim/build",
            json={
                "collection": SINGLE_COLL,
                # min_cohesion omitted: use config default so collection & pool stay aligned
            },
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
        import json

        single_doc_key = f"{SINGLE_COLL}:bin_sim:unweighted_cosine:{m1}::{m2}"
        single_doc_raw = r.get(single_doc_key)
        single_doc = json.loads(single_doc_raw) if single_doc_raw else None
        print(f"  [DEBUG] Single Collection Doc: {single_doc}")

        # ==================================================================
        section("4. Pool Analysis (Separated Collections)")
        # Create pool
        print("  Creating pool...")
        # All tuning params omitted: pool build/cluster fall back to the same config
        # defaults the collection path uses, so the two are compared on equal footing.
        config = {
            "only_cross_collection": False,
            "func_sim_params": {},
            "func_cluster_params": {},
            "file_sim_params": {"enabled": True},
            "file_cluster_params": {"enabled": True},
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
        import json

        pool_doc_key = f"global:pool:{POOL_ID}:bin_sim:unweighted_cosine:{b1[0]}:{b1[1]}::{b2[0]}:{b2[1]}"
        pool_doc_raw = r.get(pool_doc_key)
        pool_doc = json.loads(pool_doc_raw) if pool_doc_raw else None
        print(f"  [DEBUG] Pool Doc: {pool_doc}")

        # ==================================================================
        section("5. Comparison Results")
        print(f"  Single Collection Score: {single_coll_score}")
        print(f"  Pool-specific Score:     {pool_score}")
        print()
        print("  URLs for review:")
        print(f"    - Single Collection:                  {BASE_URL}/collections/{SINGLE_COLL}")
        print(f"    - Separate Collection (ARM):          {BASE_URL}/collections/{SEP_COLL_ARM}")
        print(f"    - Separate Collection (Linux):        {BASE_URL}/collections/{SEP_COLL_LINUX}")
        print(f"    - Pool:                               {BASE_URL}/pools/{POOL_ID}")
        print(f"    - Single Collection Comparison Diff:  {BASE_URL}/collections/{SINGLE_COLL}/files/{MD5_ARM}/vs/{SINGLE_COLL}/{MD5_LINUX}")
        print(f"    - Pool Comparison Diff:               {BASE_URL}/pools/{POOL_ID}/collections/{SEP_COLL_ARM}/files/{MD5_ARM}/vs/{SEP_COLL_LINUX}/{MD5_LINUX}")
        # ponytail: simple URL prints
        print()

        errors = []

        # Load similarities to build dynamic equivalence map for tie-breaking
        algo = "unweighted_cosine"
        single_func_sim_scores = r.zrange(
            f"{SINGLE_COLL}:sim:score:{algo}", 0, -1, withscores=True
        )
        pool_func_sim_scores = r.zrange(
            f"global:pool:{POOL_ID}:sim:score", 0, -1, withscores=True
        )

        def get_clean_fid(fid_bytes):
            fid = fid_bytes.decode() if isinstance(fid_bytes, bytes) else str(fid_bytes)
            func_idx = fid.find(":func:")
            if func_idx != -1:
                return fid[func_idx + 6 :]
            return fid

        single_raw_pairs = []
        for sid_b, score in single_func_sim_scores:
            sid = sid_b.decode() if isinstance(sid_b, bytes) else str(sid_b)
            parts = sid.split(f":sim:{algo}:")
            if len(parts) == 2:
                ids = parts[1].split("::")
                if len(ids) == 2:
                    single_raw_pairs.append(
                        (get_clean_fid(ids[0]), get_clean_fid(ids[1]), score)
                    )

        # Build dynamic equivalence mapping based on similarity profiles
        from collections import defaultdict

        profile_a = defaultdict(list)
        for f1, f2, score in single_raw_pairs:
            f1_in_a = MD5_ARM in f1
            f2_in_a = MD5_ARM in f2
            if f1_in_a != f2_in_a:
                if f1_in_a:
                    fa, fb = f1, f2
                else:
                    fa, fb = f2, f1
                profile_a[fa].append((fb, round(score, 4)))

        a_groups = defaultdict(list)
        for fa, prof in profile_a.items():
            a_groups[tuple(sorted(prof))].append(fa)

        canonical_map = {}
        for prof, funcs in a_groups.items():
            rep = min(funcs)
            for f in funcs:
                canonical_map[f] = rep

        profile_b = defaultdict(list)
        for f1, f2, score in single_raw_pairs:
            f1_in_a = MD5_ARM in f1
            f2_in_a = MD5_ARM in f2
            if f1_in_a != f2_in_a:
                if f1_in_a:
                    fa, fb = f1, f2
                else:
                    fa, fb = f2, f1
                profile_b[fb].append((canonical_map.get(fa, fa), round(score, 4)))

        b_groups = defaultdict(list)
        for fb, prof in profile_b.items():
            b_groups[tuple(sorted(prof))].append(fb)

        for prof, funcs in b_groups.items():
            rep = min(funcs)
            for f in funcs:
                canonical_map[f] = rep

        def canonical_func_id(fid):
            clean = get_clean_fid(fid)
            return canonical_map.get(clean, clean)

        # 5.1 Compare Binary Similarity Scores
        if single_coll_score is not None and pool_score is not None:
            s_score_rounded = round(single_coll_score, 3)
            p_score_rounded = round(pool_score, 3)
            diff = abs(s_score_rounded - p_score_rounded)
            print(
                f"  Binary similarity score difference (rounded to 3 decimals): {diff:.6f}"
            )
            if diff >= 1e-5:
                errors.append(
                    f"Binary similarity scores do not match! Single: {s_score_rounded}, Pool: {p_score_rounded}, Diff: {diff}"
                )
        else:
            errors.append("One or both binary similarity scores could not be resolved.")

        # 5.2 Compare all binary similarity documents
        if single_doc and pool_doc:

            def normalize_bin_sim_diff(diff):
                if not diff:
                    return diff
                normalized = {}
                for key in (
                    "matched",
                    "unique_to_a",
                    "unique_to_b",
                    "unclustered_a",
                    "unclustered_b",
                ):
                    if key not in diff:
                        continue
                    items = []
                    for item in diff[key]:
                        norm_item = {
                            k: v
                            for k, v in item.items()
                            if k not in ("cluster_uuid", "cluster_id", "sim_rarity", "collection_rarity", "avg_features")
                        }
                        for fkey in ("funcs_a", "funcs_b", "funcs"):
                            if fkey in norm_item and norm_item[fkey]:
                                norm_item[fkey] = sorted(
                                    [canonical_func_id(f) for f in norm_item[fkey]]
                                )
                        if "func_id" in norm_item:
                            norm_item["func_id"] = canonical_func_id(
                                norm_item["func_id"]
                            )
                        items.append(norm_item)
                    if key == "matched":
                        items.sort(
                            key=lambda x: (
                                x.get("funcs_a", [""])[0] if x.get("funcs_a") else "",
                                x.get("funcs_b", [""])[0] if x.get("funcs_b") else "",
                            )
                        )
                    elif key in ("unique_to_a", "unique_to_b"):
                        items.sort(key=lambda x: x.get("func_id", ""))
                    normalized[key] = items
                return normalized

            # Compare normalized documents
            common_keys = ("score", "md5_a", "md5_b", "diff")
            normalized_single_doc = {
                k: v for k, v in single_doc.items() if k in common_keys
            }
            normalized_pool_doc = {
                k: v for k, v in pool_doc.items() if k in common_keys
            }

            # Normalize md5 keys
            if "md5_1" in pool_doc:
                normalized_pool_doc["md5_a"] = pool_doc["md5_1"]
            if "md5_2" in pool_doc:
                normalized_pool_doc["md5_b"] = pool_doc["md5_2"]

            # Add normalized diff
            normalized_single_doc["diff"] = normalize_bin_sim_diff(
                single_doc.get("diff")
            )
            normalized_pool_doc["diff"] = normalize_bin_sim_diff(pool_doc.get("diff"))

            # Also round scores to compare
            for doc in (normalized_single_doc, normalized_pool_doc):
                for k in ("score",):
                    if k in doc and doc[k] is not None:
                        doc[k] = round(doc[k], 3)
            if normalized_single_doc != normalized_pool_doc:
                errors.append(
                    f"Binary similarity docs do not match!\n  Single: {normalized_single_doc}\n  Pool:   {normalized_pool_doc}"
                )
        else:
            errors.append("One or both binary similarity documents are missing.")

        # 5.3 Compare Function-level Similarities
        def parse_single_sid(sid_bytes):
            sid = sid_bytes.decode() if isinstance(sid_bytes, bytes) else str(sid_bytes)
            parts = sid.split(f":sim:{algo}:")
            if len(parts) == 2:
                ids = parts[1].split("::")
                if len(ids) == 2:
                    return tuple(sorted([canonical_func_id(i) for i in ids]))
            return None

        def parse_pool_sid(sid_bytes):
            sid = sid_bytes.decode() if isinstance(sid_bytes, bytes) else str(sid_bytes)
            parts = sid.split(":sim:")
            if len(parts) == 2:
                ids = parts[1].split("::")
                if len(ids) == 2:
                    return tuple(sorted([canonical_func_id(i) for i in ids]))
            return None

        single_func_map = {}
        for sid_b, score in single_func_sim_scores:
            key = parse_single_sid(sid_b)
            if key:
                single_func_map[key] = round(score, 4)

        pool_func_map = {}
        for sid_b, score in pool_func_sim_scores:
            key = parse_pool_sid(sid_b)
            if key:
                pool_func_map[key] = round(score, 4)

        # Check function similarity keys
        single_keys = set(single_func_map.keys())
        pool_keys = set(pool_func_map.keys())
        if single_keys != pool_keys:
            errors.append(
                f"Function similarity pairs do not match!\n  Only in Single: {single_keys - pool_keys}\n  Only in Pool: {pool_keys - single_keys}"
            )

        # Check function similarity values
        for key in single_keys & pool_keys:
            if abs(single_func_map[key] - pool_func_map[key]) > 1e-4:
                errors.append(
                    f"Function similarity score mismatch for {key}: Single: {single_func_map[key]}, Pool: {pool_func_map[key]}"
                )

        # Compare detailed function similarity docs
        for key in single_keys & pool_keys:
            # Reconstruct single sid from key
            # Reconstruct pool sid
            pool_sid = None
            for sid_b, _ in pool_func_sim_scores:
                parsed = parse_pool_sid(sid_b)
                if parsed == key:
                    pool_sid = (
                        sid_b.decode() if isinstance(sid_b, bytes) else str(sid_b)
                    )
                    break

            # Since key is canonical, reconstruct single_sid carefully
            # Let's find single_sid by matching parsed key
            single_sid = None
            for sid_b, _ in single_func_sim_scores:
                parsed = parse_single_sid(sid_b)
                if parsed == key:
                    single_sid = (
                        sid_b.decode() if isinstance(sid_b, bytes) else str(sid_b)
                    )
                    break

            if single_sid and pool_sid:
                s_doc = json.loads(r.get(single_sid) or "{}")
                p_doc = json.loads(r.get(pool_sid) or "{}")
                norm_s = {
                    k: v
                    for k, v in s_doc.items()
                    if k not in ("collection", "entry_date", "id1", "id2")
                }
                norm_p = {
                    k: v
                    for k, v in p_doc.items()
                    if k
                    not in (
                        "collection",
                        "entry_date",
                        "id1",
                        "id2",
                        "coll_1",
                        "coll_2",
                    )
                }
                # Round doc score
                if "score" in norm_s:
                    norm_s["score"] = round(norm_s["score"], 4)
                if "score" in norm_p:
                    norm_p["score"] = round(norm_p["score"], 4)
                if norm_s != norm_p:
                    errors.append(
                        f"Detailed function similarity docs mismatch for {key}:\n  Single: {norm_s}\n  Pool:   {norm_p}"
                    )

        # 5.4 Compare Function Clusters
        single_cids = {
            cid.decode() if isinstance(cid, bytes) else str(cid)
            for cid in r.smembers(f"{SINGLE_COLL}:cluster:list:{algo}")
        }
        pool_cids = {
            cid.decode() if isinstance(cid, bytes) else str(cid)
            for cid in r.smembers(f"global:pool:{POOL_ID}:cluster:list")
        }

        def get_clean_members(members_set):
            cleaned = []
            for m in members_set:
                cleaned.append(canonical_func_id(m))
            return tuple(sorted(cleaned))

        def normalize_cluster_meta(meta):
            normalized = {
                k: v
                for k, v in meta.items()
                if k
                not in ("collection", "id", "created_at", "cluster_uuid", "cluster_id")
            }
            if "sample_functions" in normalized:
                norm_samples = []
                for func in normalized["sample_functions"]:
                    norm_func = {
                        k: v
                        for k, v in func.items()
                        if k not in ("function_id", "collection")
                    }
                    norm_samples.append(norm_func)
                norm_samples.sort(
                    key=lambda x: (
                        x.get("entrypoint_address", ""),
                        x.get("file_md5", ""),
                    )
                )
                normalized["sample_functions"] = norm_samples
            return normalized

        single_clusters_map = {}
        for cid in single_cids:
            m_set = r.smembers(f"{SINGLE_COLL}:cluster:{algo}:{cid}:members")
            clean_m = get_clean_members(m_set)
            meta = json.loads(r.get(f"{SINGLE_COLL}:cluster:{algo}:{cid}:meta") or "{}")
            single_clusters_map[clean_m] = normalize_cluster_meta(meta)

        pool_clusters_map = {}
        for cid in pool_cids:
            m_set = r.smembers(f"global:pool:{POOL_ID}:cluster:{algo}:{cid}:members")
            clean_m = get_clean_members(m_set)
            meta = json.loads(
                r.get(f"global:pool:{POOL_ID}:cluster:{algo}:{cid}:meta") or "{}"
            )
            pool_clusters_map[clean_m] = normalize_cluster_meta(meta)

        single_clust_keys = set(single_clusters_map.keys())
        pool_clust_keys = set(pool_clusters_map.keys())

        if single_clust_keys != pool_clust_keys:
            errors.append(
                f"Function clusters do not match!\n  Only in Single: {single_clust_keys - pool_clust_keys}\n  Only in Pool: {pool_clust_keys - single_clust_keys}"
            )

        for key in single_clust_keys & pool_clust_keys:
            if single_clusters_map[key] != pool_clusters_map[key]:
                errors.append(
                    f"Metadata mismatch for cluster {key}:\n  Single: {single_clusters_map[key]}\n  Pool:   {pool_clusters_map[key]}"
                )

        # Assert no errors
        if not errors:
            print(
                "\n  ✔ SUCCESS: The similarity scores, function similarities, and clusters match perfectly!"
            )
        else:
            print("\n  ✗ FAILURE: Mismatches detected:")
            for err in errors:
                print(f"    - {err}")
            assert False, f"Comparison failed with {len(errors)} errors."

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
