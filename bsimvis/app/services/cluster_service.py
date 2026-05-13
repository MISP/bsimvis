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
        selection_method="eom",
        min_sim=0.0,
        min_features=0,
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

        prefix = f"{collection}:sim:{algo}:"

        # 1.5 Feature Filtering
        allowed_fids = None
        if min_features > 0:
            msg = f"Filtering functions by min_features={min_features}..."
            logging.info(f"[*] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            unique_fids = set()
            for sid, _ in pairs:
                if not sid.startswith(prefix):
                    continue
                ids_part = sid[len(prefix) :]
                if "::" not in ids_part:
                    continue
                c1, c2 = ids_part.split("::")
                unique_fids.add(f"{collection}:func:{c1}")
                unique_fids.add(f"{collection}:func:{c2}")

            allowed_fids = set()
            fids_list = list(unique_fids)
            # Bulk fetch bsim_features_count
            pipe = r.pipeline()
            for fid in fids_list:
                pipe.json().get(f"{fid}:meta", "$.bsim_features_count")
            
            raw_res = pipe.execute()
            for fid, res in zip(fids_list, raw_res):
                try:
                    # JSON.GET with path returns list of results
                    val = res[0] if isinstance(res, list) and res else 0
                    if int(val) >= min_features:
                        allowed_fids.add(fid)
                except (ValueError, TypeError, IndexError):
                    continue
            
            logging.info(f"[+] {len(allowed_fids)} functions passed feature filter.")

        # 2. Build identity mapping and edge list
        # We need a numeric mapping for HDBSCAN
        id_to_idx = {}
        idx_to_id = {}
        edges = []

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

            # Apply Feature Filter
            if allowed_fids is not None:
                if fid1 not in allowed_fids or fid2 not in allowed_fids:
                    continue

            for fid in [fid1, fid2]:
                if fid not in id_to_idx:
                    idx = len(id_to_idx)
                    id_to_idx[fid] = idx
                    idx_to_id[idx] = fid

            # 2.5 Apply similarity threshold if provided
            score_val = float(score)
            if min_sim > 0 and score_val < min_sim:
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
                    f"Error: No valid similarity edges found after parsing {len(pairs)} pairs. Check filters.",
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
            cluster_selection_method=selection_method,
            metric="precomputed",
            gen_min_span_tree=True,
        )
        # Only fit to get the condensed tree (no flat clustering)
        clusterer.fit(dist_matrix.astype(np.float64))
        # Calculate HDBSCAN stabilities and per-function membership strengths
        # We use the condensed tree to calculate persistence for all nodes
        tree_df = clusterer.condensed_tree_.to_pandas()
        
        # 1. Birth lambdas for all clusters
        # Root birth is 0
        root_id = tree_df['parent'].min()
        birth_lambdas = {root_id: 0.0}
        for _, row in tree_df.iterrows():
            if row['child_size'] > 1:
                birth_lambdas[int(row['child'])] = float(row["lambda_val"])
        
        # 2. Death lambdas for all clusters (max lambda of any child)
        death_lambdas = {}
        for _, row in tree_df.iterrows():
            p = int(row['parent'])
            l = float(row["lambda_val"])
            if p not in death_lambdas or l > death_lambdas[p]:
                death_lambdas[p] = l
        
        # Stability and per-point strengths will be calculated after extracting members
        pass



        # 4. Extract Condensed Tree for UI and Hierarchical Storage
        tree_df = clusterer.condensed_tree_.to_pandas()
        tree_json = tree_df.to_json(orient="records")
        tree_key = f"{collection}:cluster:tree:{algo}"
        r.set(tree_key, tree_json)
        
        # Store cluster parent-child relationships for dendrogram
        cluster_tree_key = f"{collection}:cluster:tree_links:{algo}"
        tree_links = []
        for _, row in tree_df.iterrows():
            if int(row['child_size']) > 1:
                tree_links.append({
                    "parent": int(row['parent']),
                    "child": int(row['child']),
                    "lambda": float(row['lambda_val']),
                    "size": int(row['child_size'])
                })
        r.set(cluster_tree_key, json.dumps(tree_links))

        logging.info("[*] Extracting hierarchical clusters from tree...")
        if job_service and job_id:
            job_service.add_log(job_id, "Extracting hierarchical clusters from tree...")

        # Build tree traversal mapping
        child_to_parent = dict(zip(tree_df['child'], tree_df['parent']))
        
        leaf_to_clusters = {}
        for leaf in range(num_nodes):
            clusters = set()
            curr = leaf
            while curr in child_to_parent:
                p = child_to_parent[curr]
                clusters.add(int(p))
                curr = p
            leaf_to_clusters[leaf] = list(clusters)

        # Reverse map to find cluster members
        cluster_members = {}
        for leaf, clusters in leaf_to_clusters.items():
            for c in clusters:
                if c not in cluster_members:
                    cluster_members[c] = []
                cluster_members[c].append(idx_to_id[leaf])

        label_to_uuid = {
            c: f"{uuid.uuid4().hex[:12]}" for c in cluster_members.keys()
        }

        # 5. Calculate Stability for all hierarchical nodes
        # Stability S(C) = sum_{p in C} (lambda_p_death - lambda_C_birth)
        stabilities = {}
        
        # Pre-calculate leaf deaths
        leaf_death_lambdas = {}
        for _, row in tree_df.iterrows():
            if row['child_size'] == 1:
                leaf_death_lambdas[int(row['child'])] = float(row["lambda_val"])
        
        # Calculate stability for each hierarchical cluster
        for label, members in cluster_members.items():
            b_lambda = birth_lambdas.get(label, 0.0)
            total_area = 0.0
            for fid in members:
                leaf_idx = id_to_idx[fid]
                d_lambda = leaf_death_lambdas.get(leaf_idx, b_lambda)
                total_area += max(0.0, d_lambda - b_lambda)
            stabilities[label] = total_area

        # 5. Persist function assignments
        logging.info(f"[*] Persisting {len(cluster_members)} hierarchical clusters...")
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Persisting {len(cluster_members)} hierarchical clusters and assigning functions...",
            )

        from bsimvis.app.services.index_service import _index_tag, _unindex_tag

        pipe = r.pipeline()

        # Save members set for each cluster
        for c, members in cluster_members.items():
            pipe.sadd(f"{collection}:cluster:{algo}:{c}:members", *members)
            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        func_tag_fields = [f for f in self.get_native_fields("func", False) if f.startswith("cluster_")]

        # Ensure noise functions are cleared if they were previously noise
        for i, (leaf, clusters) in enumerate(leaf_to_clusters.items()):
            fid = idx_to_id[leaf]
            clusters_key = f"{fid}:clusters"
            scores_key = f"{fid}:cluster_scores"
            
            if clusters:
                pipe.delete(clusters_key)
                pipe.sadd(clusters_key, *clusters)
                
                # Calculate scores for each cluster membership
                # Score = (lambda_death(leaf) - lambda_birth(cluster)) / (lambda_death(cluster) - lambda_birth(cluster))
                # This is a common way to define membership strength in HDBSCAN
                scores = {}
                l_death_leaf = leaf_death_lambdas.get(leaf, 0.0)
                for c in clusters:
                    l_birth_c = birth_lambdas.get(c, 0.0)
                    l_death_c = death_lambdas.get(c, l_death_leaf)
                    
                    if l_death_c > l_birth_c:
                        score = (l_death_leaf - l_birth_c) / (l_death_c - l_birth_c)
                    else:
                        score = 1.0
                    scores[str(c)] = float(max(0.0, min(1.0, score)))
                
                pipe.delete(scores_key)
                pipe.hset(scores_key, mapping=scores)
            else:
                pipe.delete(clusters_key)
                pipe.delete(scores_key)

            # Per-function scores and membership lists are updated here
            if i % 500 == 0:
                pipe.execute()
                if job_service and job_id:
                    pct = int((i / num_nodes) * 50) # First 50% for per-function persistence
                    job_service.update_progress(job_id, pct)

        pipe.execute()

        # Update secondary index for 'cluster_id', 'cluster_uuid', 'cluster_name'
        # Optimized: Iterate over clusters, not functions
        logging.info(f"[*] Updating secondary indexes for {len(cluster_members)} clusters...")
        for idx, (label, members) in enumerate(cluster_members.items()):
            if "cluster_id" in func_tag_fields:
                bucket_key = f"{collection}:idx:func:cluster_id:{str(label).lower()}"
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:func:cluster_id", bucket_key)
            
            if "cluster_uuid" in func_tag_fields:
                c_uuid = label_to_uuid[label]
                bucket_key = f"{collection}:idx:func:cluster_uuid:{c_uuid.lower()}"
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:func:cluster_uuid", bucket_key)

            if idx % 100 == 0:
                pipe.execute()
        pipe.execute()

        pipe.execute()

        # 6. Calculate Cluster Metadata
        logging.info(f"[*] Calculating enriched metadata for {len(cluster_members)} clusters...")

        all_member_fids = list(id_to_idx.keys())
        all_member_meta = {}
        total_members = len(all_member_fids)
        if job_service and job_id:
            job_service.add_log(job_id, f"Pre-fetching metadata for {total_members} functions...")
        
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
            job_service.add_log(job_id, f"Enriching metadata for {total_clusters} hierarchical clusters...")

        for idx, (label, members) in enumerate(cluster_members.items()):
            names = []
            feature_counts = []

            for fid in members:
                m = all_member_meta.get(fid, {})
                if m.get("function_name"):
                    names.append(m["function_name"])
                if "bsim_features_count" in m:
                    feature_counts.append(m.get("bsim_features_count", 0))

            default_name = Counter(names).most_common(1)[0][0] if names else f"Cluster {label}"
            avg_features = np.mean(feature_counts) if feature_counts else 0
            
            # Cohesion Score based on Birth Lambda (Threshold at which cluster forms)
            # Higher lambda means higher density/similarity.
            # sim = 1 - 1/lambda
            b_lambda = birth_lambdas.get(label, 0.0)
            cohesion_score = 1.0 - (1.0 / b_lambda) if b_lambda > 1.0 else 0.0
            
            # Cap cohesion score at 1.0 and ensure it's not negative
            cohesion_score = max(0.0, min(1.0, cohesion_score))

            # Find representative function name/snippet
            rep_fid = members[0] if members else None
            rep_meta = all_member_meta.get(rep_fid, {}) if rep_fid else {}
            snippet = rep_meta.get("function_name", "unknown")

            meta = {
                "cluster_id": int(label),
                "snippet": snippet,
                "cluster_uuid": label_to_uuid[label],
                "cluster_name": default_name,
                "avg_features": float(avg_features),
                "cohesion_score": float(cohesion_score),
                "avg_stability": float(stabilities.get(label, 0.0)),
                "cluster_stability": float(stabilities.get(label, 0.0)),
                "member_count": len(members),
                "sample_members": names[:5],  # Include a few actual function names
                "created_at": int(time.time() * 1000),
            }

            # Sanitize metadata to ensure no NaN/inf values (invalid JSON)
            for k, v in meta.items():
                if isinstance(v, float):
                    if not np.isfinite(v):
                        meta[k] = 0.0
            pipe.json().set(f"{collection}:cluster:{algo}:{label}:meta", "$", meta)
            
            if "cluster_name" in func_tag_fields:
                bucket_key = f"{collection}:idx:func:cluster_name:{default_name.lower()}"
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:func:cluster_name", bucket_key)

            if job_service and job_id and (idx + 1) % 50 == 0:
                pct = 50 + int(((idx + 1) / total_clusters) * 50)
                job_service.update_progress(
                    job_id, pct, f"Enriching clusters: {idx + 1}/{total_clusters}"
                )

        if job_service and job_id:
            job_service.add_log(job_id, f"Writing cluster metadata to database...")
        pipe.execute()

        # 7. Update all similarities in the collection to propagate cluster info
        self._update_similarity_indexing(
            collection, algo, job_service=job_service, job_id=job_id
        )

        noise_count = sum(1 for clusters in leaf_to_clusters.values() if not clusters)
        summary = f"Clustering complete. Found {len(cluster_members)} hierarchical clusters. Noise: {noise_count} functions."
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
            if cid == "noise" and not r.exists(members_key):
                members_key = f"{collection}:idx:func:cluster_id:noise"

            members = r.smembers(members_key)
            if members:
                pipe = r.pipeline()
                for j, mid_raw in enumerate(members):
                    mid = mid_raw.decode() if isinstance(mid_raw, bytes) else mid_raw

                    _unindex_tag(pipe, collection, "func", "cluster_id", cid, mid)
                    pipe.delete(f"{mid}:clusters")

                    if j % 500 == 0:
                        pipe.execute()
                pipe.execute()

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
            # We now fetch clusters from the SET and meta from JSON
            for fid_raw in chunk:
                fid = fid_raw.decode() if isinstance(fid_raw, bytes) else fid_raw
                pipe.smembers(f"{fid}:clusters")
                pipe.json().get(f"{fid}:meta", "$")
                
            results = pipe.execute()
            
            # Since we have 2 commands per chunk item, results are grouped
            for idx, fid_raw in enumerate(chunk):
                fid = fid_raw.decode() if isinstance(fid_raw, bytes) else fid_raw
                clusters_res = results[idx * 2]
                meta_res = results[idx * 2 + 1]
                
                # Decode cluster IDs
                cluster_ids = [int(c) for c in clusters_res] if clusters_res else []
                
                meta_entry = {}
                if meta_res:
                    m = meta_res[0] if isinstance(meta_res, list) else meta_res
                    if isinstance(m, str):
                        m = json.loads(m)
                    for f in fields_to_fetch:
                        if f != "cluster_id":
                            meta_entry[f] = m.get(f)
                
                # Add cluster_id to meta_entry explicitly
                if cluster_ids:
                    meta_entry["cluster_id"] = cluster_ids
                    
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
