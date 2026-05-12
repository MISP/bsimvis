import logging
import json
import time
import uuid
from collections import Counter
import numpy as np
from bsimvis.app.services.redis_client import get_redis

try:
    import hdbscan
except ImportError:
    hdbscan = None


class ClusterService:
    def __init__(self, r=None):
        self.r = r or get_redis()
        from bsimvis.app.services.index_config import get_native_fields, get_propagated_fields
        self.get_native_fields = get_native_fields
        self.get_propagated_fields = get_propagated_fields

    def run_clustering(
        self,
        collection,
        algo="unweighted_cosine",
        min_cluster_size=5,
        min_samples=None,
        cluster_selection_epsilon=0.0,
        cluster_selection_method="eom",
        similarity_threshold=0.0,
        job_service=None,
        job_id=None,
    ):
        """
        Runs HDBSCAN clustering on similarity pairs stored in Kvrocks.
        """
        if hdbscan is None:
            logging.error(
                "hdbscan library not installed. Please install it to use clustering."
            )
            return False

        r = self.r
        sim_score_key = f"{collection}:sim:score:{algo}"

        # 1. Fetch all similarity pairs
        logging.info(f"[*] Fetching similarity pairs from {sim_score_key}...")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Fetching similarity pairs for {collection} ({algo})..."
            )

        # Use ZSCAN to be safe with large datasets
        pairs = []
        cursor = 0
        while True:
            cursor, results = r.zscan(sim_score_key, cursor=cursor, count=1000)
            for sid, score in results:
                pairs.append((sid.decode() if isinstance(sid, bytes) else sid, score))
            if cursor == 0:
                break

        if not pairs:
            logging.warning(f"No similarity pairs found for {collection}:{algo}")
            return True

        # 2. Build identity mapping and edge list
        # We need a numeric mapping for HDBSCAN
        id_to_idx = {}
        idx_to_id = {}
        edges = []

        prefix = f"{collection}:sim:{algo}:"

        for sid, score in pairs:
            # sid format: {coll}:sim:{algo}:{clean_id1}::{clean_id2}
            if not sid.startswith(prefix):
                continue

            ids_part = sid[len(prefix) :]
            if "::" not in ids_part:
                continue

            c1, c2 = ids_part.split("::")
            fid1 = f"{collection}:func:{c1}"
            fid2 = f"{collection}:func:{c2}"

            for fid in [fid1, fid2]:
                if fid not in id_to_idx:
                    idx = len(id_to_idx)
                    id_to_idx[fid] = idx
                    idx_to_id[idx] = fid

            # 2.5 Apply similarity threshold if provided
            score_val = float(score)
            if similarity_threshold > 0 and score_val < similarity_threshold:
                continue

            # HDBSCAN works with distance. Distance = 1 - score (for normalized cosine)
            dist = max(0, 1.0 - score_val)
            edges.append((id_to_idx[fid1], id_to_idx[fid2], dist))

        if not edges:
            logging.warning(
                f"No valid edges found for {collection}:{algo} after parsing {len(pairs)} pairs."
            )
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"Error: No valid similarity edges found after parsing {len(pairs)} pairs. Check key format.",
                )
            return True

        num_nodes = len(id_to_idx)
        msg = f"Building graph with {num_nodes} functions and {len(edges)} similarity edges..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # 3. Run HDBSCAN
        # For graph-based data, we use a condensed distance matrix or just provide the edges
        # However, hdbscan.HDBSCAN(metric='precomputed') expects a full distance matrix.
        # For scalability, we use the 'geometric' approach if we have vectors,
        # but here we only have the graph.

        # Alternative: Build a sparse distance matrix
        # But HDBSCAN precomputed requires a full dense matrix usually.
        # If the dataset is huge, we might need a different approach.
        # For now, let's use a dense matrix with a large default distance (1.0)

        dist_matrix = np.ones((num_nodes, num_nodes), dtype=np.float32)
        np.fill_diagonal(dist_matrix, 0)

        for i, j, d in edges:
            dist_matrix[i, j] = d
            dist_matrix[j, i] = d

        logging.info(f"[*] Running HDBSCAN (min_cluster_size={min_cluster_size})...")
        if job_service and job_id:
            job_service.add_log(job_id, "Running HDBSCAN algorithm...")

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            cluster_selection_epsilon=cluster_selection_epsilon,
            cluster_selection_method=cluster_selection_method,
            metric="precomputed",
            gen_min_span_tree=True,
        )
        cluster_labels = clusterer.fit_predict(dist_matrix.astype(np.float64))

        # 4. Extract Condensed Tree for UI
        tree = clusterer.condensed_tree_.to_pandas()
        tree_json = tree.to_json(orient="records")
        tree_key = f"{collection}:cluster:tree:{algo}"
        r.set(tree_key, tree_json)

        # 5. Persist results
        logging.info(f"[*] Persisting {len(cluster_labels)} function assignments...")
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Persisting {len(cluster_labels)} cluster assignments and calculating metadata...",
            )

        from bsimvis.app.services.index_service import (
            _index_tag,
            _unindex_tag,
            _index_num,
        )

        cluster_stabilities = clusterer.probabilities_
        pipe = r.pipeline()

        # Maps for enrichment
        cluster_members = {}  # label -> list of fids
        label_to_uuid = {
            int(l): f"{uuid.uuid4().hex[:12]}" for l in set(cluster_labels) if l != -1
        }

        # 5. Persist function assignments
        # Discover fields from config
        func_tag_fields = [f for f in self.get_native_fields("func", False) if f.startswith("cluster_")]
        func_num_fields = [f for f in self.get_native_fields("func", True) if f.startswith("cluster_")]

        for i, label in enumerate(cluster_labels):
            fid = idx_to_id[i]
            stability = float(cluster_stabilities[i])
            label = int(label)

            if label != -1:
                cluster_uuid = label_to_uuid[label]
                cluster_id = label  # Use numerical label as cluster_id

                if label not in cluster_members:
                    cluster_members[label] = []
                cluster_members[label].append(fid)

                # Update function metadata
                pipe.json().set(f"{fid}:meta", "$.cluster_id", cluster_id)
                pipe.json().set(f"{fid}:meta", "$.cluster_uuid", cluster_uuid)
                pipe.json().set(f"{fid}:meta", "$.cluster_stability", stability)

                # Update Secondary Indexes (Config-driven)
                m_vals = {"cluster_id": cluster_id, "cluster_uuid": cluster_uuid, "cluster_stability": stability}
                for f in func_tag_fields:
                    if f in m_vals:
                        _index_tag(pipe, collection, "func", f, m_vals[f], fid)
                for f in func_num_fields:
                    if f in m_vals:
                        _index_num(pipe, collection, "func", f, m_vals[f], fid)

                # Add to cluster membership set
                pipe.sadd(f"{collection}:cluster:{algo}:{cluster_id}:members", fid)
            else:
                # Noise
                pipe.json().set(f"{fid}:meta", "$.cluster_id", "noise")
                pipe.json().set(f"{fid}:meta", "$.cluster_uuid", "noise")
                pipe.json().set(f"{fid}:meta", "$.cluster_name", "noise")
                pipe.json().set(f"{fid}:meta", "$.cluster_stability", 0.0)
                
                # Index noise
                for f in func_tag_fields:
                    _index_tag(pipe, collection, "func", f, "noise", fid)
                for f in func_num_fields:
                    _index_num(pipe, collection, "func", f, 0.0, fid)

            # Periodically execute pipeline
            if i % 500 == 0:
                pipe.execute()
                if job_service and job_id:
                    pct = int((i / num_nodes) * 100)
                    job_service.update_progress(job_id, pct)

        # Final execute for functions
        pipe.execute()

        # 6. Calculate Cluster Metadata & Similarity Metrics
        logging.info(
            f"[*] Calculating enriched metadata for {len(cluster_members)} clusters..."
        )

        # Pre-calculate internal similarities for each cluster
        cluster_similarities = {l: [] for l in cluster_members.keys()}
        for idx1, idx2, dist in edges:
            l1 = cluster_labels[idx1]
            l2 = cluster_labels[idx2]
            if l1 != -1 and l1 == l2:
                cluster_similarities[l1].append(1.0 - float(dist))

        # Pre-fetch metadata for ALL cluster members in one batched pipeline
        # (avoids N sequential round-trips to Kvrocks, one per cluster)
        all_member_fids = [fid for members in cluster_members.values() for fid in members]
        all_member_meta = {}
        total_members = len(all_member_fids)
        if job_service and job_id:
            job_service.add_log(job_id, f"Pre-fetching metadata for {total_members} cluster members...")
        for i in range(0, total_members, 1000):
            chunk = all_member_fids[i : i + 1000]
            m_pipe = r.pipeline()
            for fid in chunk:
                m_pipe.json().get(f"{fid}:meta", "$")
            for fid, res in zip(chunk, m_pipe.execute()):
                if res and isinstance(res, list) and res[0]:
                    all_member_meta[fid] = res[0]

        total_clusters = len(cluster_members)
        if job_service and job_id:
            job_service.add_log(job_id, f"Enriching metadata for {total_clusters} clusters...")

        for idx, (label, members) in enumerate(cluster_members.items()):
            names = []
            feature_counts = []
            stabilities = []

            for fid in members:
                m = all_member_meta.get(fid, {})
                if m.get("function_name"):
                    names.append(m["function_name"])
                if "bsim_features_count" in m:
                    feature_counts.append(m.get("bsim_features_count", 0))
                if "cluster_stability" in m:
                    stabilities.append(m.get("cluster_stability", 0.0))

            # Most common name
            default_name = (
                Counter(names).most_common(1)[0][0] if names else f"Cluster {label}"
            )
            avg_features = np.mean(feature_counts) if feature_counts else 0
            avg_stability = np.mean(stabilities) if stabilities else 0

            # Cohesion score (average similarity of internal edges)
            sims = cluster_similarities.get(label, [])
            cohesion_score = np.mean(sims) if sims else 0.0

            meta = {
                "cluster_id": label,
                "cluster_uuid": label_to_uuid[label],
                "cluster_name": default_name,
                "avg_stability": float(avg_stability),
                "avg_features": float(avg_features),
                "cohesion_score": float(cohesion_score),
                "member_count": len(members),
                "created_at": int(time.time() * 1000),
            }
            pipe.json().set(f"{collection}:cluster:{algo}:{label}:meta", "$", meta)

            # Index members for cluster_name and update function metadata
            for fid in members:
                pipe.json().set(f"{fid}:meta", "$.cluster_name", default_name)
                if "cluster_name" in func_tag_fields:
                    _index_tag(pipe, collection, "func", "cluster_name", default_name, fid)

            if job_service and job_id and (idx + 1) % 50 == 0:
                pct = int(((idx + 1) / total_clusters) * 100)
                job_service.update_progress(
                    job_id, pct, f"Enriching clusters: {idx + 1}/{total_clusters}"
                )

        if job_service and job_id:
            job_service.add_log(job_id, f"Writing cluster metadata to database...")
        # Final execute for metadata
        pipe.execute()

        # 7. Update all similarities in the collection to propagate cluster info
        self._update_similarity_indexing(
            collection, algo, job_service=job_service, job_id=job_id
        )

        summary = f"Clustering complete. Found {len(cluster_members)} clusters. Noise: {list(cluster_labels).count(-1)} functions."
        logging.info(f"[+] {summary}")
        if job_service and job_id:
            job_service.add_log(job_id, summary)

        return True

    def clear_clustering(
        self, collection, algo="unweighted_cosine", job_service=None, job_id=None
    ):
        """
        Clears all clustering data for a collection and algorithm.
        """
        r = self.r

        # 1. Discover all cluster meta keys
        pattern = f"{collection}:cluster:{algo}:*:meta"
        cursor = 0
        all_meta_keys = []
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            all_meta_keys.extend(
                [k.decode() if isinstance(k, bytes) else k for k in keys]
            )
            if cursor == 0:
                break

        # 2. Extract cluster IDs
        cluster_ids = []
        prefix = f"{collection}:cluster:{algo}:"
        for k in all_meta_keys:
            cid = k[len(prefix) : -len(":meta")]
            cluster_ids.append(cid)

        # Also include 'noise' if it exists in the index
        cluster_ids.append("noise")

        total_clusters = len(cluster_ids)
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Cleaning up clustering data for {total_clusters} clusters (including noise)...",
            )

        from bsimvis.app.services.index_service import _unindex_tag, _unindex_num

        # 0. Clear similarity-level cluster indexes first
        self._update_similarity_indexing(
            collection, algo, job_service=job_service, job_id=job_id, is_clear=True
        )

        # 3. For each cluster, clear members
        for i, cid in enumerate(cluster_ids):
            members_key = f"{collection}:cluster:{algo}:{cid}:members"

            # Fallback for noise: use the index itself if the members set doesn't exist
            if cid == "noise" and not r.exists(members_key):
                members_key = f"{collection}:idx:func:cluster_id:noise"

            members = r.smembers(members_key)

            if members:
                # Fetch metadata for all members to get their cluster_id and cluster_uuid
                m_pipe = r.pipeline()
                for mid_raw in members:
                    mid = mid_raw.decode() if isinstance(mid_raw, bytes) else mid_raw
                    m_pipe.json().get(f"{mid}:meta", "$")

                meta_results = m_pipe.execute()

                pipe = r.pipeline()
                for j, (mid_raw, res) in enumerate(zip(members, meta_results)):
                    mid = mid_raw.decode() if isinstance(mid_raw, bytes) else mid_raw

                    if res and isinstance(res, list) and res[0]:
                        m = res[0]
                        old_cid = m.get("cluster_id")
                        old_uuid = m.get("cluster_uuid")
                        old_name = m.get("cluster_name")

                        # Remove from secondary indexes
                        if old_cid is not None:
                            _unindex_tag(
                                pipe, collection, "func", "cluster_id", old_cid, mid
                            )
                        if old_uuid is not None:
                            _unindex_tag(
                                pipe, collection, "func", "cluster_uuid", old_uuid, mid
                            )
                        if old_name is not None:
                            _unindex_tag(
                                pipe, collection, "func", "cluster_name", old_name, mid
                            )

                        _unindex_num(pipe, collection, "func", "cluster_stability", mid)

                    # Update function metadata
                    pipe.json().set(f"{mid}:meta", "$.cluster_id", None)
                    pipe.json().set(f"{mid}:meta", "$.cluster_uuid", None)
                    pipe.json().set(f"{mid}:meta", "$.cluster_name", None)
                    pipe.json().set(f"{mid}:meta", "$.cluster_stability", None)

                    if j % 500 == 0:
                        pipe.execute()
                pipe.execute()

            # Delete cluster-specific keys
            if cid != "noise":
                r.delete(f"{collection}:cluster:{algo}:{cid}:members")
                r.delete(f"{collection}:cluster:{algo}:{cid}:meta")

            if job_service and job_id and i % 10 == 0:
                pct = int((i / total_clusters) * 100)
                job_service.update_progress(job_id, pct)

        # 4. Delete tree
        r.delete(f"{collection}:cluster:tree:{algo}")

        if job_service and job_id:
            job_service.add_log(job_id, "Clustering data cleared successfully.")
            job_service.update_progress(job_id, 100)

        return True

    def _clear_indexes_via_registry(self, collection, level, field):
        """Delete all index buckets for a field using its registry, then clear the registry."""
        r = self.r
        reg_key = f"{collection}:reg:{level}:{field}"
        buckets = r.smembers(reg_key)
        if buckets:
            pipe = r.pipeline()
            for b_raw in buckets:
                b = b_raw.decode() if isinstance(b_raw, bytes) else b_raw
                pipe.delete(b)
            pipe.execute()
        r.delete(reg_key)

    def _update_similarity_indexing(
        self, collection, algo, job_service=None, job_id=None, is_clear=False
    ):
        """
        Updates sim-level cluster indexes/registries only — no JSON writes to similarity docs.
        On clear: wipes the sim cluster indexes via registry based on config.
        On build: fetches function cluster metadata and re-indexes each similarity if propagation is enabled.
        """
        r = self.r
        sim_score_key = f"{collection}:sim:score:{algo}"

        from bsimvis.app.services.index_service import _index_tag, _index_num, _unindex_tag, _unindex_num
        from bsimvis.app.services.index_config import NUM_FIELDS

        # Discover which cluster fields are propagated from func to sim
        propagated = self.get_propagated_fields("sim")["func"]
        cluster_prop = [p for p in propagated if p[0].startswith("cluster_")]
        
        # Also check numeric fields (stability)
        func_num_fields = self.get_native_fields("func", True)
        cluster_prop_num = []
        for f in func_num_fields:
            if f.startswith("cluster_"):
                # Check if it propagates to sim
                from bsimvis.app.services.index_config import INDEX_CONFIG
                if "sim" in INDEX_CONFIG.get("func", {}).get(f, []):
                    cluster_prop_num.append(f)

        if not cluster_prop and not cluster_prop_num:
            if job_service and job_id:
                job_service.add_log(job_id, "No cluster fields are configured to propagate to similarities. Skipping scan.")
            return True

        if is_clear:
            if job_service and job_id:
                job_service.add_log(
                    job_id, "Clearing sim cluster indexes via registry..."
                )
            for orig, target in cluster_prop:
                self._clear_indexes_via_registry(collection, "sim", target)
            for f in cluster_prop_num:
                # Numeric indexes don't have registry, but we can wipe the ZSET
                r.delete(f"{collection}:idx:sim:{f}")
            return True

        # BUILD PATH
        # 0. Wipe existing sim cluster indexes to avoid stale entries
        for orig, target in cluster_prop:
            self._clear_indexes_via_registry(collection, "sim", target)
        for f in cluster_prop_num:
            r.delete(f"{collection}:idx:sim:{f}")

        # 1. Fetch function cluster metadata into memory
        if job_service and job_id:
            job_service.add_log(job_id, "Fetching function metadata for similarity re-indexing...")

        all_funcs = r.smembers(f"{collection}:all_functions")
        func_meta = {}
        funcs_list = list(all_funcs)
        
        # Fields we need to fetch
        fields_to_fetch = [p[0] for p in cluster_prop] + cluster_prop_num

        for i in range(0, len(funcs_list), 1000):
            chunk = funcs_list[i : i + 1000]
            pipe = r.pipeline()
            for fid_raw in chunk:
                fid = fid_raw.decode() if isinstance(fid_raw, bytes) else fid_raw
                pipe.json().get(f"{fid}:meta", "$")
            results = pipe.execute()
            for fid_raw, res in zip(chunk, results):
                fid = fid_raw.decode() if isinstance(fid_raw, bytes) else fid_raw
                if res:
                    m = res[0] if isinstance(res, list) else res
                    if isinstance(m, str):
                        m = json.loads(m)
                    # Only keep configured fields to save memory
                    meta_entry = {}
                    for f in fields_to_fetch:
                        meta_entry[f] = m.get(f)
                    func_meta[fid] = meta_entry

        # 2. Scan all similarities and rebuild cluster indexes from function metadata
        # Build a set of clean IDs for functions that actually have cluster info.
        # Sims where neither function is clustered are skipped entirely.
        clustered_clean_ids = set()
        func_prefix = f"{collection}:func:"
        for fid, m in func_meta.items():
            # If any of the tracked fields are present, it's a clustered function
            if any(v is not None for v in m.values()):
                clustered_clean_ids.add(fid[len(func_prefix):])

        cursor = 0
        total_sims = r.zcard(sim_score_key)
        processed = 0
        indexed = 0

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Propagating cluster indexes to {total_sims} similarities "
                f"({len(clustered_clean_ids)} clustered functions)...",
            )

        prefix = f"{collection}:sim:{algo}:"
        update_pipe = r.pipeline()
        while True:
            cursor, results = r.zscan(sim_score_key, cursor=cursor, count=5000)
            if not results:
                if cursor == 0:
                    break
                continue

            for sid_raw, score in results:
                sid = sid_raw.decode() if isinstance(sid_raw, bytes) else sid_raw

                id_part = sid[len(prefix):]
                if "::" not in id_part:
                    continue
                c1, c2 = id_part.split("::")

                # Skip if neither function has a cluster assignment
                if c1 not in clustered_clean_ids and c2 not in clustered_clean_ids:
                    continue

                fid1 = f"{collection}:func:{c1}"
                fid2 = f"{collection}:func:{c2}"
                m1 = func_meta.get(fid1, {})
                m2 = func_meta.get(fid2, {})

                # Index TAG fields
                for orig, target in cluster_prop:
                    v1 = m1.get(orig)
                    v2 = m2.get(orig)
                    values = [v for v in [v1, v2] if v is not None]
                    if values:
                        _index_tag(update_pipe, collection, "sim", target, values, sid)
                
                # Index NUM fields
                for f in cluster_prop_num:
                    v1 = m1.get(f)
                    v2 = m2.get(f)
                    # Standard logic for propagated numeric fields: index the max?
                    vals = [float(v) for v in [v1, v2] if v is not None]
                    if vals:
                        _index_num(update_pipe, collection, "sim", f, max(vals), sid)

                indexed += 1

            processed += len(results)

            # Flush every 5000 processed sims
            if processed % 5000 == 0:
                update_pipe.execute()
                update_pipe = r.pipeline()
                if job_service and job_id:
                    pct = int((processed / total_sims) * 100) if total_sims > 0 else 100
                    job_service.update_progress(
                        job_id, pct,
                        f"Scanning similarities: {processed}/{total_sims} ({indexed} indexed)"
                    )

            if cursor == 0:
                break

        update_pipe.execute()

        if job_service and job_id:
            job_service.add_log(
                job_id, f"Indexed {indexed} similarities with cluster info."
            )

        return True


cluster_service = ClusterService()
