import time
import json
import logging
import math
from collections import defaultdict
from bsimvis.app.services.redis_client import get_redis


def _index_bin_sim_pair(pipe, collection, sid, doc, file_meta_a=None, file_meta_b=None):
    """Write secondary indexes for a bin_sim pair doc."""

    def tag_index(field, value):
        if value is None:
            return
        values = value if isinstance(value, list) else [value]
        for v in values:
            if v is None or v == "":
                continue
            bucket_key = f"{collection}:idx:bin_sim:{field}:{str(v).lower()}"
            pipe.sadd(bucket_key, sid)
            registry_key = f"{collection}:reg:bin_sim:{field}"
            pipe.sadd(registry_key, bucket_key)

    def num_index(field, value):
        if value is None:
            return
        try:
            pipe.zadd(f"{collection}:idx:bin_sim:{field}", {sid: float(value)})
        except (ValueError, TypeError):
            pass

    # Tag indexes from doc
    tag_index("md5_a", doc.get("md5_a"))
    tag_index("md5_b", doc.get("md5_b"))
    tag_index("algo", doc.get("algo"))

    # Denormalized file metadata
    if file_meta_a:
        tag_index("file_name_a", file_meta_a.get("file_name"))
        tag_index("file_tags_a", file_meta_a.get("tags"))
        tag_index("file_user_tags_a", file_meta_a.get("user_tags"))
        tag_index("architecture_a", file_meta_a.get("language_id"))
    if file_meta_b:
        tag_index("file_name_b", file_meta_b.get("file_name"))
        tag_index("file_tags_b", file_meta_b.get("tags"))
        tag_index("file_user_tags_b", file_meta_b.get("user_tags"))
        tag_index("architecture_b", file_meta_b.get("language_id"))

    # Numeric indexes
    num_index("score", doc.get("score"))
    num_index("score_sim_weighted", doc.get("score_sim_weighted"))
    num_index("score_collection_weighted", doc.get("score_collection_weighted"))
    num_index("coverage_a", doc.get("coverage_a"))
    num_index("coverage_b", doc.get("coverage_b"))
    num_index("shared_clusters", doc.get("shared_clusters"))
    num_index("computed_at", doc.get("computed_at"))
    num_index("functions_count_a", doc.get("functions_count_a"))
    num_index("functions_count_b", doc.get("functions_count_b"))

    pipe.sadd(f"{collection}:all_bin_sims", sid)


def _unindex_bin_sim_pair(
    pipe, collection, sid, doc, file_meta_a=None, file_meta_b=None
):
    """Remove secondary indexes for a bin_sim pair doc."""

    def tag_unindex(field, value):
        if value is None:
            return
        values = value if isinstance(value, list) else [value]
        for v in values:
            if v is None or v == "":
                continue
            bucket_key = f"{collection}:idx:bin_sim:{field}:{str(v).lower()}"
            pipe.srem(bucket_key, sid)

    tag_unindex("md5_a", doc.get("md5_a"))
    tag_unindex("md5_b", doc.get("md5_b"))
    tag_unindex("algo", doc.get("algo"))
    if file_meta_a:
        tag_unindex("file_name_a", file_meta_a.get("file_name"))
        tag_unindex("file_tags_a", file_meta_a.get("tags"))
        tag_unindex("file_user_tags_a", file_meta_a.get("user_tags"))
        tag_unindex("architecture_a", file_meta_a.get("language_id"))
    if file_meta_b:
        tag_unindex("file_name_b", file_meta_b.get("file_name"))
        tag_unindex("file_tags_b", file_meta_b.get("tags"))
        tag_unindex("file_user_tags_b", file_meta_b.get("user_tags"))
        tag_unindex("architecture_b", file_meta_b.get("language_id"))

    for num_field in [
        "score",
        "score_sim_weighted",
        "score_collection_weighted",
        "coverage_a",
        "coverage_b",
        "shared_clusters",
        "computed_at",
        "functions_count_a",
        "functions_count_b",
    ]:
        pipe.zrem(f"{collection}:idx:bin_sim:{num_field}", sid)

    pipe.srem(f"{collection}:all_bin_sims", sid)


class BinSimService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def build_bin_sim(
        self,
        collection,
        algo="unweighted_cosine",
        md5_a=None,
        md5_b=None,
        min_cohesion=0.5,
        job_service=None,
        job_id=None,
    ):
        """
        Builds binary similarity diff docs and scores for pairs of binaries.
        Uses a cluster-first greedy sweep algorithm.
        """
        r = self.r
        start_time = time.time()

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Starting Binary Similarity Build for collection {collection} (algo: {algo})",
            )

        # 1. Fetch all files (binaries)
        file_keys = []
        if md5_a and md5_b:
            binaries = [md5_a, md5_b]
        else:
            # Get all md5s
            all_files_key = f"{collection}:all_files"
            file_keys = [
                d.decode() if isinstance(d, bytes) else str(d)
                for d in r.smembers(all_files_key)
            ]

            binaries = []
            for k in file_keys:
                if k.endswith(":meta"):
                    continue
                parts = k.split(":")
                if len(parts) >= 3:
                    binaries.append(parts[2])
            binaries = list(set(binaries))

        num_binaries = len(binaries)
        if num_binaries < 2:
            msg = "Not enough binaries to compare."
            if job_service and job_id:
                job_service.add_log(job_id, msg)
                job_service.update_progress(job_id, 100)
            return True

        # 2. Build cluster frequency map for rarity
        # We need to know for each cluster, how many distinct binaries have it.
        if job_service and job_id:
            job_service.add_log(job_id, "[*] Precomputing cluster rarities...")

        binary_cluster_maps = {}
        cluster_binary_count_job = defaultdict(int)
        binary_fids = {}
        # ponytail: reverse map fid -> set(cluster labels) so matched pairs can be tagged with a cluster
        fid_clusters = defaultdict(set)

        binary_func_counts = {}
        for i, md5 in enumerate(binaries):
            func_set_key = f"{collection}:idx:file:functions:{md5}"
            raw_ids = r.smembers(func_set_key)
            binary_func_counts[md5] = len(raw_ids)
            fids = [
                (
                    fid.decode().replace(":meta", "")
                    if isinstance(fid, bytes)
                    else str(fid).replace(":meta", "")
                )
                for fid in raw_ids
            ]
            binary_fids[md5] = set(fids)

            # Map of cid -> set of function IDs for this binary
            b_cluster_map = defaultdict(set)

            if fids:
                pipe = r.pipeline(transaction=False)
                for fid in fids:
                    if collection.startswith("global:pool:"):
                        pipe.smembers(f"{collection}:{fid}:clusters")
                    else:
                        pipe.smembers(f"{fid}:clusters")

                results = pipe.execute()

                for idx, fid in enumerate(fids):
                    clusters_res = results[idx]
                    if clusters_res:
                        for c_raw in clusters_res:
                            cid = (
                                c_raw.decode()
                                if isinstance(c_raw, bytes)
                                else str(c_raw)
                            )
                            b_cluster_map[cid].add(fid)
                            fid_clusters[fid].add(cid)

            binary_cluster_maps[md5] = b_cluster_map
            for cid in b_cluster_map.keys():
                cluster_binary_count_job[cid] += 1

            if job_service and job_id and (i + 1) % 50 == 0:
                job_service.update_progress(
                    job_id,
                    int((i + 1) / num_binaries * 10),
                    f"Loading cluster maps: {i+1}/{num_binaries}",
                )

        # Load cluster metadata (uuid/name/cohesion) for every cluster seen, so matched
        # function pairs can be tagged with their best-matching function cluster.
        # `algo` names the function similarity these clusters were built from, so
        # bin_sim reads and writes inside that same namespace.
        cluster_meta = {}
        all_labels = list(cluster_binary_count_job.keys())
        if all_labels:
            pipe = r.pipeline(transaction=False)
            for lbl in all_labels:
                pipe.get(f"{collection}:cluster:{algo}:{lbl}:meta")
            for lbl, res in zip(all_labels, pipe.execute()):
                if not res:
                    continue
                m = json.loads(res.decode() if isinstance(res, bytes) else res)
                if isinstance(m, str):
                    m = json.loads(m)
                if isinstance(m, dict):
                    cluster_meta[lbl] = m

        def _pick_label(candidates):
            """Among candidate cluster labels, pick the one with tightest cohesion."""
            best = None
            best_coh = -1.0
            for lbl in candidates:
                meta = cluster_meta.get(lbl)
                if not meta:
                    continue
                coh = float(meta.get("cohesion_score", 0.0))
                if coh > best_coh:
                    best_coh = coh
                    best = lbl
            return best

        def pick_cluster_label(fid_a, fid_b):
            """Best function cluster label for a matched pair: prefer a cluster both
            share, else any cluster either belongs to; tie-break on tightest cohesion.
            """
            la = fid_clusters.get(fid_a, set())
            lb = fid_clusters.get(fid_b, set())
            shared = la & lb
            return _pick_label(shared if shared else (la | lb))

        def pick_cluster(fid_a, fid_b):
            """Best-matching cluster meta for a matched pair (name/uuid for the UI)."""
            lbl = pick_cluster_label(fid_a, fid_b)
            return cluster_meta.get(lbl) if lbl else None

        def get_col_rarity(cid):
            # Rarity from how many distinct binaries in the collection share this
            # function cluster: a rarer cluster => higher score. Primary source is the
            # collection-wide unique_files_count (set during HDBSCAN); fall back to the
            # local job count when that field is missing, and to maximal rarity when the
            # function belongs to no cluster at all.
            if not cid:
                # ponytail: no cluster => function does not recur across the collection,
                # so treat it as maximally rare. Upgrade path: per-function similarity
                # neighbour count if a cheaper signal than clustering is ever stored.
                return 1.0
            global_count = cluster_meta.get(cid, {}).get(
                "unique_files_count", cluster_binary_count_job.get(cid, 0)
            )
            return min(1.0, 1.0 / math.log(1 + global_count + 1))

        # 3. Load function metadata (for bsim_features_count & names)
        func_meta_cache = {}
        all_unique_fids = set()
        for fids_set in binary_fids.values():
            all_unique_fids.update(fids_set)

        if all_unique_fids:
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] Loading metadata for {len(all_unique_fids)} functions...",
                )
            fids_list = list(all_unique_fids)
            pipe = r.pipeline(transaction=False)
            for fid in fids_list:
                pipe.get(f"{fid}:meta")
            meta_results = pipe.execute()
            for fid, res in zip(fids_list, meta_results):
                if res:
                    m = json.loads(res) if not isinstance(res, dict) else res
                    if isinstance(m, str):
                        try:
                            m = json.loads(m)
                        except ValueError:
                            pass
                    func_meta_cache[fid] = m if isinstance(m, dict) else {}

        # 4. Generate Pairs
        pairs = []
        if md5_a and md5_b:
            if md5_a < md5_b:
                pairs.append((md5_a, md5_b))
            else:
                pairs.append((md5_b, md5_a))
        else:
            for i in range(len(binaries)):
                for j in range(i + 1, len(binaries)):
                    b1, b2 = binaries[i], binaries[j]
                    if b1 < b2:
                        pairs.append((b1, b2))
                    else:
                        pairs.append((b2, b1))

        num_pairs = len(pairs)
        if job_service and job_id:
            job_service.add_log(
                job_id, f"[*] Computing similarities for {num_pairs} pairs..."
            )

        # 5. Process Pairs (Direct Similarity Matching with Bipartite Greedy Selection)
        processed = 0
        pipe = r.pipeline(transaction=False)
        pair_scores = {}

        # Pre-fetch file metadata for all binaries (for indexing)
        file_meta_cache = {}
        pipe_meta = r.pipeline(transaction=False)
        for md5 in binaries:
            pipe_meta.get(f"{collection}:file:{md5}:meta")
        meta_results = pipe_meta.execute()
        for md5, res in zip(binaries, meta_results):
            if res:
                m = json.loads(res) if not isinstance(res, dict) else res
                if isinstance(m, str):
                    m = json.loads(m)
                file_meta_cache[md5] = m if isinstance(m, dict) else {}
            else:
                file_meta_cache[md5] = {}

        # ponytail: Determine if this collection is a pool or a normal collection
        is_pool = collection.startswith("global:pool:") or collection.startswith(
            "pool:"
        )
        if is_pool:
            from bsimvis.app.services.index_service import get_pool_id

            pool_id = get_pool_id(collection)
            involves_file_prefix = f"global:pool:{pool_id}:sim:involves:file:"
        else:
            involves_file_prefix = f"{collection}:sim:involves:file:"

        for m_a, m_b in pairs:
            file_meta_a = file_meta_cache.get(m_a, {})
            file_meta_b = file_meta_cache.get(m_b, {})

            # ponytail: Use Kvrocks SINTER to fetch similarities involving both files without temporary keys
            involves_a = f"{involves_file_prefix}{m_a}"
            involves_b = f"{involves_file_prefix}{m_b}"
            sim_keys = [
                k.decode() if isinstance(k, bytes) else str(k)
                for k in r.sinter(involves_a, involves_b)
            ]

            # Fetch similarity documents in parallel
            sim_docs = []
            if sim_keys:
                pipe_sim = r.pipeline(transaction=False)
                for k in sim_keys:
                    pipe_sim.get(k)
                sim_res = pipe_sim.execute()
                for res in sim_res:
                    if res:
                        sim_docs.append(
                            json.loads(res.decode() if isinstance(res, bytes) else res)
                        )

            # Filter/extract edges
            edges = []
            for doc in sim_docs:
                fid1 = doc.get("id1")
                fid2 = doc.get("id2")
                score = doc.get("score", 0.0)
                if fid1 and fid2:
                    # ponytail: Extract exact MD5 out of function IDs (casing-insensitive)
                    parts1 = fid1.split(":")
                    parts2 = fid2.split(":")
                    if len(parts1) >= 2 and len(parts2) >= 2:
                        m1 = parts1[-2].lower()
                        m2 = parts2[-2].lower()
                        m_a_clean = m_a.lower()
                        m_b_clean = m_b.lower()
                        if m1 == m_a_clean and m2 == m_b_clean:
                            edges.append((fid1, fid2, score))
                        elif m1 == m_b_clean and m2 == m_a_clean:
                            edges.append((fid2, fid1, score))

            # Sort edges by score descending (greedy match prioritizes best matches), using function IDs as deterministic tie-breakers
            edges.sort(key=lambda x: (-x[2], x[0], x[1]))

            assigned_a = set()
            assigned_b = set()
            diff_matched = []

            sum_weighted_cohesion_sim = 0.0
            sum_weights_sim = 0.0

            sum_weighted_cohesion_col = 0.0
            sum_weights_col = 0.0

            sum_weighted_cohesion_unweighted = 0.0
            sum_weights_unweighted = 0.0

            # Match greedily
            for fid_a, fid_b, score in edges:
                if fid_a not in assigned_a and fid_b not in assigned_b:
                    assigned_a.add(fid_a)
                    assigned_b.add(fid_b)

                    f_features_a = float(
                        func_meta_cache.get(fid_a, {}).get("bsim_features_count", 1.0)
                    )
                    f_features_b = float(
                        func_meta_cache.get(fid_b, {}).get("bsim_features_count", 1.0)
                    )
                    f_features = max(f_features_a, f_features_b)

                    # Slim doc: persist only the stable/expensive triple. Cluster tag +
                    # rarity are derived live at read (get_bin_sim) from current cluster
                    # meta, so a cluster rebuild can't leave them stale. [[Change 1]]
                    diff_matched.append(
                        {
                            "similarity": score,
                            "avg_features": f_features,
                            "func_a": fid_a,
                            "func_b": fid_b,
                        }
                    )

                    sum_weighted_cohesion_sim += score * f_features
                    sum_weights_sim += f_features

                    sum_weighted_cohesion_col += score * f_features
                    sum_weights_col += f_features

                    sum_weighted_cohesion_unweighted += score * f_features
                    sum_weights_unweighted += f_features

            all_funcs_a_total = binary_fids[m_a]
            all_funcs_b_total = binary_fids[m_b]

            unassigned_a = all_funcs_a_total - assigned_a
            unassigned_b = all_funcs_b_total - assigned_b

            unique_to_a = []
            for fid in sorted(list(unassigned_a)):
                f_features = float(
                    func_meta_cache.get(fid, {}).get("bsim_features_count", 1.0)
                )
                if f_features <= 0:
                    f_features = 1.0

                # Slim: cluster tag + rarity derived at read. [[Change 1]]
                unique_to_a.append(
                    {
                        "func_id": fid,
                        "avg_features": f_features,
                    }
                )
                sum_weights_sim += f_features
                sum_weights_col += f_features
                sum_weights_unweighted += f_features

            unique_to_b = []
            for fid in sorted(list(unassigned_b)):
                f_features = float(
                    func_meta_cache.get(fid, {}).get("bsim_features_count", 1.0)
                )
                if f_features <= 0:
                    f_features = 1.0

                # Slim: cluster tag + rarity derived at read. [[Change 1]]
                unique_to_b.append(
                    {
                        "func_id": fid,
                        "avg_features": f_features,
                    }
                )
                sum_weights_sim += f_features
                sum_weights_col += f_features
                sum_weights_unweighted += f_features

            score_sim_weighted = (
                sum_weighted_cohesion_sim / sum_weights_sim
                if sum_weights_sim > 0
                else 0.0
            )
            score_collection_weighted = (
                sum_weighted_cohesion_col / sum_weights_col
                if sum_weights_col > 0
                else 0.0
            )
            score_unweighted = (
                sum_weighted_cohesion_unweighted / sum_weights_unweighted
                if sum_weights_unweighted > 0
                else 0.0
            )

            cov_a = (
                len(assigned_a) / len(all_funcs_a_total) if all_funcs_a_total else 0.0
            )
            cov_b = (
                len(assigned_b) / len(all_funcs_b_total) if all_funcs_b_total else 0.0
            )

            sid = f"{collection}:bin_sim:{algo}:{m_a}::{m_b}"
            pair_scores[(m_a, m_b)] = score_collection_weighted

            doc = {
                "md5_a": m_a,
                "md5_b": m_b,
                "algo": algo,
                "architecture_a": file_meta_a.get("language_id", ""),
                "architecture_b": file_meta_b.get("language_id", ""),
                "functions_count_a": binary_func_counts.get(m_a, 0),
                "functions_count_b": binary_func_counts.get(m_b, 0),
                "score": score_unweighted,
                "score_sim_weighted": score_sim_weighted,
                "score_collection_weighted": score_collection_weighted,
                "coverage_a": cov_a,
                "coverage_b": cov_b,
                "shared_clusters": len(diff_matched),
                "unique_clusters_a": len(unique_to_a),
                "unique_clusters_b": len(unique_to_b),
                "unclustered_a": len(unique_to_a),
                "unclustered_b": len(unique_to_b),
                "computed_at": int(time.time() * 1000),
                "diff": {
                    "matched": diff_matched,
                    "unique_to_a": unique_to_a,
                    "unique_to_b": unique_to_b,
                    "unclustered_a": [],
                    "unclustered_b": [],
                },
            }

            pipe.set(sid, json.dumps(doc))
            # `algo` is a provenance tag (which function similarity the clusters came
            # from), not a choice of file score. The sort score is always the
            # unweighted cohesion mean so it means the same thing in every namespace
            # and matches the pool-level score. The other aggregates stay in `doc`.
            final_bin_score = score_unweighted

            pipe.zadd(f"{collection}:bin_sim:score:{algo}", {sid: final_bin_score})
            pipe.sadd(f"{collection}:bin_sim:involves:{m_a}", sid)
            pipe.sadd(f"{collection}:bin_sim:involves:{m_b}", sid)
            pipe.sadd(f"{collection}:bin_sim:built:{algo}", sid)

            # Secondary indexes
            _index_bin_sim_pair(pipe, collection, sid, doc, file_meta_a, file_meta_b)

            processed += 1

            if processed % 100 == 0:
                pipe.execute()
                if job_service and job_id:
                    pct = 10 + int(processed / num_pairs * 80)
                    job_service.update_progress(
                        job_id, pct, f"Processed {processed}/{num_pairs} pairs"
                    )

        pipe.execute()

        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Completed binary similarity build for {processed} pairs."
            )

        return True

    def clear_bin_sim(
        self,
        collection,
        algo="unweighted_cosine",
        md5=None,
        job_service=None,
        job_id=None,
    ):
        """
        Clears binary similarity scores.
        If md5 is provided, clears only pairs involving that md5.
        """
        r = self.r
        if job_service and job_id:
            job_service.add_log(
                job_id, f"[*] Clearing binary similarities (md5: {md5 or 'ALL'})"
            )

        if md5:
            involves_key = f"{collection}:bin_sim:involves:{md5}"
            sids = r.smembers(involves_key)
            if sids:
                pipe = r.pipeline(transaction=False)
                for sid_raw in sids:
                    sid = sid_raw.decode() if isinstance(sid_raw, bytes) else sid_raw
                    pipe.delete(sid)
                    pipe.zrem(f"{collection}:bin_sim:score:{algo}", sid)
                    pipe.srem(f"{collection}:bin_sim:built:{algo}", sid)

                    parts = sid.split(":")
                    if len(parts) >= 5:
                        m_a, m_b = (
                            parts[4].split("::")
                            if "::" in parts[4]
                            else (parts[3], parts[4])
                        )
                        # Let's cleanly extract it
                        try:
                            keys_split = sid.split(f"{collection}:bin_sim:{algo}:")[
                                1
                            ].split("::")
                            m_a, m_b = keys_split[0], keys_split[1]
                            other_md5 = m_b if m_a == md5 else m_a
                            pipe.srem(f"{collection}:bin_sim:involves:{other_md5}", sid)
                        except:
                            pass

                pipe.delete(involves_key)
                pipe.execute()
        else:
            patterns = [
                f"{collection}:bin_sim:{algo}:*",
                f"{collection}:bin_sim:involves:*",
            ]

            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                    if keys:
                        r.delete(*keys)
                    if cursor == 0:
                        break

            r.delete(f"{collection}:bin_sim:score:{algo}")
            r.delete(f"{collection}:bin_sim:built:{algo}")

        if job_service and job_id:
            job_service.update_progress(job_id, 100, "Cleared binary similarities.")

        return True

    def reindex_bin_sim(
        self, collection, algo="unweighted_cosine", job_service=None, job_id=None
    ):
        """
        Rebuilds secondary indexes for all existing bin_sim pairs in the collection.
        Use after deploy or when indexes are missing.
        """
        r = self.r
        if job_service and job_id:
            job_service.add_log(
                job_id, f"[*] Reindexing bin_sim pairs for collection {collection}"
            )

        built_key = f"{collection}:bin_sim:built:{algo}"
        sids = list(r.smembers(built_key))
        if not sids:
            if job_service and job_id:
                job_service.add_log(job_id, "No bin_sim docs found to reindex.")
                job_service.update_progress(job_id, 100)
            return True

        sids = [s.decode() if isinstance(s, bytes) else s for s in sids]
        total = len(sids)

        # Pre-fetch all file meta we might need
        md5s = set()
        for sid in sids:
            try:
                rest = sid.split(f"bin_sim:{algo}:")[1]
                m_a, m_b = rest.split("::")
                md5s.update([m_a, m_b])
            except Exception:
                pass

        file_meta_cache = {}
        md5_list = list(md5s)
        if md5_list:
            pipe_meta = r.pipeline(transaction=False)
            for md5 in md5_list:
                pipe_meta.get(f"{collection}:file:{md5}:meta")
            for md5, res in zip(md5_list, pipe_meta.execute()):
                if res:
                    m = json.loads(res) if not isinstance(res, dict) else res
                    if isinstance(m, str):
                        m = json.loads(m)
                    file_meta_cache[md5] = m if isinstance(m, dict) else {}
                else:
                    file_meta_cache[md5] = {}

        # Fetch all docs and reindex
        pipe = r.pipeline(transaction=False)
        for sid in sids:
            pipe.get(sid)
        docs = pipe.execute()

        pipe = r.pipeline(transaction=False)
        for i, (sid, res) in enumerate(zip(sids, docs)):
            if not res:
                continue
            doc = json.loads(res) if not isinstance(res, dict) else res
            if isinstance(doc, str):
                doc = json.loads(doc)
            m_a = doc.get("md5_a", "")
            m_b = doc.get("md5_b", "")
            _index_bin_sim_pair(
                pipe,
                collection,
                sid,
                doc,
                file_meta_cache.get(m_a),
                file_meta_cache.get(m_b),
            )
            if (i + 1) % 200 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
                if job_service and job_id:
                    job_service.update_progress(job_id, int((i + 1) / total * 100))

        pipe.execute()

        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Reindexed {total} bin_sim pairs."
            )
        return True


bin_sim_service = BinSimService()
