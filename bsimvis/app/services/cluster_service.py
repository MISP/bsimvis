import logging
import json
import os
import time
import uuid
from collections import Counter, defaultdict
import numpy as np
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.cluster_utils import pick_best_shared_cluster
from bsimvis.app.services import mem_util, sim_edges

# Above this many nodes a dense size^2 float64 distance matrix stops being
# survivable (0.8 GiB at 10k, 3.2 GiB at 20k) on a 3 GB per-worker cap.
CLUSTER_MAX_COMPONENT = int(os.getenv("CLUSTER_MAX_COMPONENT", 10000))

_EMPTY_I = np.empty(0, dtype=np.int32)
_EMPTY_F = np.empty(0, dtype=np.float32)

try:
    import hdbscan
except ImportError:
    hdbscan = None


class ClusterService:
    def __init__(self, r=None):
        self.r = r or get_redis()
        from bsimvis.app.services.index_config import (
            get_native_fields,
            get_propagated_fields,
        )

        self.get_native_fields = get_native_fields
        self.get_propagated_fields = get_propagated_fields

    def run_clustering(
        self,
        collection,
        algo="unweighted_cosine",
        min_cluster_size=None,
        min_samples=None,
        cluster_selection_epsilon=None,
        selection_method=None,
        min_sim=None,
        min_features=None,
        job_service=None,
        job_id=None,
    ):
        """
        Runs HDBSCAN clustering on similarity pairs stored in Kvrocks.
        """
        from bsimvis.app.services.config_service import config_service

        if min_cluster_size is None:
            min_cluster_size = config_service.get("clustering.min_cluster_size", 2)
        if min_samples is None:
            min_samples = config_service.get("clustering.min_samples", 1)
        if cluster_selection_epsilon is None:
            cluster_selection_epsilon = config_service.get("clustering.epsilon", 0.1)
        if selection_method is None:
            selection_method = config_service.get("clustering.selection_method", "eom")
        if min_sim is None:
            min_sim = config_service.get("clustering.min_sim", 0.0)
        if min_features is None:
            min_features = config_service.get("clustering.min_features", 0)
        if hdbscan is None:
            logging.error(
                "hdbscan library not installed. Please install it to use clustering."
            )
            return False

        is_pool = collection.startswith("global:pool:")

        if is_pool:
            pool_id = collection[len("global:pool:") :]
            sim_score_key = f"global:pool:{pool_id}:sim:score"
            prefix = f"global:pool:{pool_id}:sim:"
        else:
            sim_score_key = f"{collection}:sim:score:{algo}"
            prefix = f"{collection}:sim:{algo}:"

        r = self.r

        # 1. Fetch all similarity pairs
        logging.info(f"[*] Fetching similarity pairs from {sim_score_key}...")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Fetching similarity pairs for {collection} ({algo})..."
            )

        # 1.5 Feature Filtering
        allowed_fids = None
        if min_features > 0:
            msg = f"Filtering functions by min_features={min_features}..."
            logging.info(f"[*] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            allowed_fids = sim_edges.collect_allowed_fids(
                r, sim_score_key, prefix, is_pool, collection, min_features
            )
            logging.info(f"[+] {len(allowed_fids)} functions passed feature filter.")

        # 2. Stream the ZSET straight into typed edge arrays.
        # This used to build a `pairs` list of every member string first, then a
        # second list of edge tuples. Measured on a real 5.4M-pair pool that was
        # 2.15 GiB before clustering even started, against a 3 GB worker cap.
        edge_set = sim_edges.load_edges(
            r,
            sim_score_key,
            prefix,
            is_pool,
            collection,
            min_sim=min_sim,
            allowed_fids=allowed_fids,
        )
        id_to_idx = edge_set.id_to_idx
        idx_to_id = edge_set.idx_to_id

        msg = f"Fetched {edge_set.n_scanned} similarity pairs."
        logging.info(f"[+] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)
        mem_util.phase(f"after streaming {edge_set.n_scanned} pairs", job_service, job_id)

        if edge_set.n_scanned == 0:
            logging.warning(f"No similarity pairs found for {collection}:{algo}")
            return True

        if edge_set.src.size == 0:
            logging.warning(
                f"No valid edges found for {collection}:{algo} after parsing {edge_set.n_scanned} pairs."
            )
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"Error: No valid similarity edges found after parsing {edge_set.n_scanned} pairs. Check filters.",
                )
            return True

        num_nodes = len(id_to_idx)
        msg = f"Building graph with {num_nodes} functions and {edge_set.src.size} similarity edges..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)
        mem_util.phase(f"after building {edge_set.src.size} edges", job_service, job_id)

        # 3. Connected Components and Local HDBSCAN
        from scipy.sparse.csgraph import connected_components
        import pandas as pd

        msg = f"Shattering graph into connected components to avoid OOM..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        adj_matrix = sim_edges.build_adjacency(edge_set, num_nodes)
        mem_util.phase("after adjacency matrix", job_service, job_id)
        n_components, labels = connected_components(csgraph=adj_matrix, directed=False)
        del adj_matrix

        comp_to_nodes = {}
        for i, comp_id in enumerate(labels):
            if comp_id not in comp_to_nodes:
                comp_to_nodes[comp_id] = []
            comp_to_nodes[comp_id].append(i)

        # Views into one sorted permutation rather than a dict of tuple lists,
        # which was a third full copy of every edge.
        comp_to_edges = sim_edges.group_edges_by_component(edge_set, labels)

        mem_util.phase("after comp_to_edges", job_service, job_id)

        msg = f"Found {n_components} connected components. Running local HDBSCAN..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)
        biggest = max((len(v) for v in comp_to_nodes.values()), default=0)
        logging.info(f"[*] Largest connected component: {biggest} nodes")

        global_tree_rows = []
        global_root_id = num_nodes
        next_cluster_id = num_nodes + 1
        comp_roots = []

        # One scratch buffer for global-index -> component-local-index, reused
        # across components. Allocating it per component would reintroduce a
        # per-component O(num_nodes) allocation.
        gmap = np.full(num_nodes, -1, dtype=np.int32)

        start_fit = time.time()

        for comp_id, comp_nodes in comp_to_nodes.items():
            size = len(comp_nodes)
            if size < min_cluster_size:
                for node in comp_nodes:
                    comp_roots.append((node, 1))
                continue

            sub_id_to_global = {
                i: global_idx for i, global_idx in enumerate(comp_nodes)
            }

            # Global index -> position within this component, vectorised. The
            # per-edge Python loop that did this built three more lists per
            # component on top of the edge copy it was reading from.
            comp_nodes_arr = np.asarray(comp_nodes, dtype=np.int32)
            gmap[comp_nodes_arr] = np.arange(size, dtype=np.int32)
            e_src, e_dst, e_dist = comp_to_edges.get(comp_id, (_EMPTY_I, _EMPTY_I, _EMPTY_F))
            ui = gmap[e_src]
            vi = gmap[e_dst]

            if size >= 5000:
                from scipy.sparse.linalg import svds
                import scipy.sparse as sp

                sim = 1.0 - e_dist
                rows_sp = np.concatenate([ui, vi])
                cols_sp = np.concatenate([vi, ui])
                data_sp = np.concatenate([sim, sim])

                comp_matrix = sp.csr_matrix(
                    (data_sp, (rows_sp, cols_sp)), shape=(size, size), dtype=np.float32
                )
                comp_matrix.setdiag(1.0)

                k = min(50, size - 1)
                u, s, vt = svds(comp_matrix, k=k)
                embeddings = u @ np.diag(np.sqrt(s))
                del comp_matrix, rows_sp, cols_sp, data_sp

                clusterer = hdbscan.HDBSCAN(
                    min_cluster_size=min(min_cluster_size, size),
                    min_samples=min(min_samples, size),
                    cluster_selection_epsilon=cluster_selection_epsilon,
                    cluster_selection_method=selection_method,
                    metric="euclidean",
                    gen_min_span_tree=True,
                )
                clusterer.fit(embeddings)
            else:
                # float64 up front. HDBSCAN's precomputed path needs float64
                # anyway, so building this float32 and converting at fit time
                # held BOTH the 100 MB original and its 200 MB copy alive at
                # size=5000 -- 300 MB where 200 MB does the same work.
                sub_dist = np.ones((size, size), dtype=np.float64)
                np.fill_diagonal(sub_dist, 0)

                if ui.size:
                    sub_dist[ui, vi] = e_dist
                    sub_dist[vi, ui] = e_dist

                clusterer = hdbscan.HDBSCAN(
                    min_cluster_size=min(min_cluster_size, size),
                    min_samples=min(min_samples, size),
                    cluster_selection_epsilon=cluster_selection_epsilon,
                    cluster_selection_method=selection_method,
                    metric="precomputed",
                    gen_min_span_tree=True,
                )
                clusterer.fit(sub_dist)

            local_tree_df = clusterer.condensed_tree_.to_pandas()
            if local_tree_df.empty:
                for node in comp_nodes:
                    comp_roots.append((node, 1))
                continue

            sub_internal_to_global = {}
            # Ensure local root maps to a single global internal ID
            local_root_sub = local_tree_df["parent"].min()

            for row in local_tree_df.itertuples(index=False):
                parent = int(row.parent)
                child = int(row.child)

                if parent not in sub_internal_to_global:
                    sub_internal_to_global[parent] = next_cluster_id
                    next_cluster_id += 1

                if child < size:  # Leaf
                    global_child = sub_id_to_global[child]
                else:  # Internal
                    if child not in sub_internal_to_global:
                        sub_internal_to_global[child] = next_cluster_id
                        next_cluster_id += 1
                    global_child = sub_internal_to_global[child]

                global_tree_rows.append(
                    {
                        "parent": sub_internal_to_global[parent],
                        "child": global_child,
                        "lambda_val": float(row.lambda_val),
                        "child_size": int(row.child_size),
                    }
                )

            comp_roots.append((sub_internal_to_global[local_root_sub], size))

        fit_time = time.time() - start_fit

        msg = f"HDBSCAN fit completed in {fit_time:.2f}s."
        logging.info(f"[+] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # Stitch all roots to a synthetic global root at lambda 1.0 (distance 1.0)
        for comp_root, size in comp_roots:
            global_tree_rows.append(
                {
                    "parent": global_root_id,
                    "child": comp_root,
                    "lambda_val": 1.0,
                    "child_size": size,
                }
            )

        tree_df = pd.DataFrame(global_tree_rows)

        msg = f"Global condensed tree has {len(tree_df)} rows."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # 1. Birth lambdas for all clusters
        # Root birth is 0
        root_id = tree_df["parent"].min()
        birth_lambdas = {root_id: 0.0}
        for row in tree_df.itertuples(index=False):
            if row.child_size > 1:
                birth_lambdas[int(row.child)] = float(row.lambda_val)

        # 2. Death lambdas for all clusters (max lambda of any child)
        death_lambdas = {}
        for row in tree_df.itertuples(index=False):
            p = int(row.parent)
            l = float(row.lambda_val)
            if p not in death_lambdas or l > death_lambdas[p]:
                death_lambdas[p] = l

        # Stability and per-point strengths will be calculated after extracting members
        pass

        # Pruning tree based on cluster_selection_epsilon (if > 0)
        pruned_clusters = set()
        if cluster_selection_epsilon and cluster_selection_epsilon > 0.0:
            lambda_threshold = 1.0 / cluster_selection_epsilon
            for c, b_lambda in birth_lambdas.items():
                if b_lambda > lambda_threshold:
                    pruned_clusters.add(c)

        child_to_parent = dict(zip(tree_df["child"], tree_df["parent"]))

        def get_nearest_non_pruned_ancestor(node):
            curr = node
            while curr in child_to_parent:
                p = child_to_parent[curr]
                if p not in pruned_clusters:
                    return p
                curr = p
            return None

        # Build a pruned tree DataFrame
        if pruned_clusters:
            pruned_rows = []
            for row in tree_df.itertuples(index=False):
                parent = int(row.parent)
                child = int(row.child)
                child_size = int(row.child_size)
                lambda_val = float(row.lambda_val)

                if parent in pruned_clusters:
                    ancestor = get_nearest_non_pruned_ancestor(parent)
                    if ancestor is not None:
                        parent = ancestor
                    else:
                        continue  # Skip if no ancestor

                if child_size > 1:
                    if child in pruned_clusters:
                        continue

                pruned_rows.append(
                    {
                        "parent": parent,
                        "child": child,
                        "lambda_val": lambda_val,
                        "child_size": child_size,
                    }
                )
            import pandas as pd

            tree_df = pd.DataFrame(pruned_rows)

        # 4. Extract Condensed Tree for UI and Hierarchical Storage
        tree_json = tree_df.to_json(orient="records")
        tree_key = f"{collection}:cluster:tree:{algo}"
        r.set(tree_key, tree_json)

        # Store cluster parent-child relationships for dendrogram
        cluster_tree_key = f"{collection}:cluster:tree_links:{algo}"
        tree_links = []
        for row in tree_df.itertuples(index=False):
            if int(row.child_size) > 1:
                tree_links.append(
                    {
                        "parent": int(row.parent),
                        "child": int(row.child),
                        "lambda": float(row.lambda_val),
                        "size": int(row.child_size),
                    }
                )
        r.set(cluster_tree_key, json.dumps(tree_links))

        logging.info("[*] Extracting hierarchical clusters from tree...")
        if job_service and job_id:
            job_service.add_log(job_id, "Extracting hierarchical clusters from tree...")

        # Map leaves to the clusters they actually survive into (shed noise
        # points excluded). See cluster_common.hierarchical_membership.
        from bsimvis.app.services.cluster_common import hierarchical_membership

        leaf_to_clusters, leaf_home = hierarchical_membership(
            tree_df, num_nodes, global_root_id, min_size=min_cluster_size
        )

        # Reverse map to find cluster members
        cluster_members = {}
        for leaf, clusters in leaf_to_clusters.items():
            for c in clusters:
                if c not in cluster_members:
                    cluster_members[c] = []
                cluster_members[c].append(idx_to_id[leaf])

        label_to_uuid = {c: f"{uuid.uuid4().hex[:12]}" for c in cluster_members.keys()}

        # 5. Calculate Stability for all hierarchical nodes
        # Stability S(C) = sum_{p in C} (lambda_p_death - lambda_C_birth)
        stabilities = {}

        # Pre-calculate leaf deaths
        leaf_death_lambdas = {}
        for row in tree_df.itertuples(index=False):
            if row.child_size == 1:
                leaf_death_lambdas[int(row.child)] = float(row.lambda_val)

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

        pipe = r.pipeline(transaction=False)

        # Save members set for each cluster
        for c, members in cluster_members.items():
            pipe.sadd(f"{collection}:cluster:{algo}:{c}:members", *members)
            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        # Direct members = leaves whose deepest surviving cluster is this node
        # (shed noise points are excluded, matching the membership rule above).
        direct_members = {}
        for leaf, p in leaf_home.items():
            if leaf in idx_to_id:
                direct_members.setdefault(p, []).append(idx_to_id[leaf])

        for c, d_members in direct_members.items():
            pipe.sadd(f"{collection}:cluster:{algo}:{c}:direct_members", *d_members)
            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        func_tag_fields = [
            f for f in self.get_native_fields("func", False) if f.startswith("cluster_")
        ]

        # Ensure noise functions are cleared if they were previously noise
        for i, (leaf, clusters) in enumerate(leaf_to_clusters.items()):
            fid = idx_to_id[leaf]
            if is_pool:
                clusters_key = f"{collection}:{fid}:clusters"
                scores_key = f"{collection}:{fid}:cluster_scores"
            else:
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
                    pct = int(
                        (i / num_nodes) * 50
                    )  # First 50% for per-function persistence
                    job_service.update_progress(job_id, pct)

        pipe.execute()

        # Update secondary index for 'cluster_id', 'cluster_uuid', 'cluster_name'
        # Optimized: Iterate over clusters, not functions
        logging.info(
            f"[*] Updating secondary indexes for {len(cluster_members)} clusters..."
        )
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
        logging.info(
            f"[*] Calculating enriched metadata for {len(cluster_members)} clusters..."
        )

        all_member_fids = list(id_to_idx.keys())
        all_member_meta = {}
        total_members = len(all_member_fids)
        msg = f"Pre-fetching metadata for {total_members} functions..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        for i in range(0, total_members, 1000):
            chunk = all_member_fids[i : i + 1000]
            m_pipe = r.pipeline(transaction=False)
            for fid in chunk:
                m_pipe.get(f"{fid}:meta")
            results = m_pipe.execute()
            for idx, fid in enumerate(chunk):
                raw_meta = results[idx]
                m = {}
                if raw_meta:
                    try:
                        m = json.loads(raw_meta)
                    except Exception:
                        m = {}

                name = m.get("function_name")
                feat_count = m.get("bsim_features_count", 0)
                file_name = m.get("file_name")
                addr = m.get("entrypoint_address")
                md5 = m.get("file_md5")

                all_member_meta[fid] = {
                    "function_name": name,
                    "bsim_features_count": feat_count,
                    "file_name": file_name,
                    "entrypoint_address": addr,
                    "file_md5": md5,
                }

            if i % 5000 == 0:
                logging.info(f"[*] Fetched meta for {i}/{total_members} functions...")

        # Build sparse adjacency dictionary of similarities for fast cohesion calculation
        msg = f"Building sparse adjacency map for cohesion calculation..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # CSR, not a dict of dicts. Measured on the real 11.3M-pair pool, the
        # dict version held 1.08 GiB and took the run from 1.70 GiB to 2.78 GiB
        # against a 3 GB cap -- the single largest structure left in clustering.
        adj_sim = sim_edges.SimAdjacency(edge_set, num_nodes)
        mem_util.phase("after adj_sim", job_service, job_id)

        total_clusters = len(cluster_members)
        msg = f"Enriching metadata for {total_clusters} hierarchical clusters..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        for idx, (label, members) in enumerate(cluster_members.items()):
            names = []
            feature_counts = []

            for fid in members:
                m = all_member_meta.get(fid, {})
                if m.get("function_name"):
                    names.append(m["function_name"])
                if "bsim_features_count" in m:
                    feature_counts.append(m.get("bsim_features_count", 0))

            default_name = (
                Counter(names).most_common(1)[0][0] if names else f"Cluster {label}"
            )
            avg_features = np.mean(feature_counts) if feature_counts else 0

            # Exact Average Internal Similarity (Cohesion) using sparse adjacency map
            if len(members) > 1:
                member_indices = [id_to_idx[fid] for fid in members]
                n_members = len(members)

                total_sim = adj_sim.cohesion_sum(member_indices)
                cohesion_score = total_sim / (n_members * (n_members - 1) / 2.0)
            else:
                cohesion_score = 1.0

            unique_md5s = set()
            for fid in members:
                # fid format: collection:func:md5:address
                parts = fid.split(":")
                if len(parts) >= 3:
                    unique_md5s.add(parts[2])

            # Find representative function name/snippet
            rep_fid = members[0] if members else None
            rep_meta = all_member_meta.get(rep_fid, {}) if rep_fid else {}
            snippet = rep_meta.get("function_name", "unknown")

            samples = []
            for fid in members[:5]:
                m = all_member_meta.get(fid, {})
                samples.append(
                    {
                        "function_id": fid,
                        "function_name": m.get("function_name", "Unknown"),
                        "entrypoint_address": m.get("entrypoint_address"),
                        "file_md5": m.get("file_md5"),
                        "file_name": m.get("file_name"),
                        "collection": collection,
                        "bsim_features_count": m.get("bsim_features_count", 0),
                    }
                )

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
                "unique_files_count": len(unique_md5s),
                "sample_functions": samples,
                "created_at": int(time.time() * 1000),
            }

            # Sanitize metadata to ensure no NaN/inf values (invalid JSON)
            for k, v in meta.items():
                if isinstance(v, float):
                    if not np.isfinite(v):
                        meta[k] = 0.0
            pipe.set(f"{collection}:cluster:{algo}:{label}:meta", json.dumps(meta))

            if "cluster_name" in func_tag_fields:
                bucket_key = (
                    f"{collection}:idx:func:cluster_name:{default_name.lower()}"
                )
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

        # Maintain a set of all active cluster IDs for fast listing
        cluster_list_key = f"{collection}:cluster:list:{algo}"
        r.delete(cluster_list_key)
        if cluster_members:
            r.sadd(cluster_list_key, *[str(k) for k in cluster_members.keys()])
            if is_pool:
                pool_cluster_list_key = f"global:pool:{pool_id}:cluster:list"
                r.delete(pool_cluster_list_key)
                r.sadd(pool_cluster_list_key, *[str(k) for k in cluster_members.keys()])

        # Free the graph structures before propagation: even as CSR, adj_sim
        # holds two entries per edge (~22M on the 11.3M-pair pool), and keeping
        # it alive through the whole sim-index phase used to leave the worker
        # swapping.
        del adj_sim, comp_to_edges, edge_set, all_member_meta

        logging.info(f"Update sim indexes...")

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
        cluster_list_key = f"{collection}:cluster:list:{algo}"
        cids_raw = r.smembers(cluster_list_key)
        all_meta_keys = []
        if cids_raw:
            all_meta_keys = [
                f"{collection}:cluster:{algo}:{cid.decode() if isinstance(cid, bytes) else cid}:meta"
                for cid in cids_raw
            ]
        else:
            pattern = f"{collection}:cluster:{algo}:*:meta"
            cursor = 0
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

        logging.info(f"[*] Updating similarity index...")

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
                pipe = r.pipeline(transaction=False)
                for j, mid_raw in enumerate(members):
                    mid = mid_raw.decode() if isinstance(mid_raw, bytes) else mid_raw

                    _unindex_tag(pipe, collection, "func", "cluster_id", cid, mid)
                    if collection.startswith("global:pool:"):
                        pipe.delete(f"{collection}:{mid}:clusters")
                        pipe.delete(f"{collection}:{mid}:cluster_scores")
                    else:
                        pipe.delete(f"{mid}:clusters")
                        pipe.delete(f"{mid}:cluster_scores")

                    if j % 500 == 0:
                        pipe.execute()
                pipe.execute()

            if cid != "noise":
                r.delete(f"{collection}:cluster:{algo}:{cid}:members")
                r.delete(f"{collection}:cluster:{algo}:{cid}:direct_members")
                r.delete(f"{collection}:cluster:{algo}:{cid}:meta")

            if job_service and job_id and i % 10 == 0:
                pct = int((i / total_clusters) * 100)
                job_service.update_progress(job_id, pct)

        # 4. Delete tree and cluster list
        r.delete(f"{collection}:cluster:tree:{algo}")
        r.delete(f"{collection}:cluster:list:{algo}")

        if job_service and job_id:
            job_service.add_log(job_id, "Clustering data cleared successfully.")
            job_service.update_progress(job_id, 100)

        return True

    def _clear_indexes_via_registry(self, collection, level, field):
        """Delete all index buckets for a field using its registry, then clear the registry."""
        r = self.r
        reg_key = f"{collection}:reg:{level}:{field}"
        buckets = list(r.smembers(reg_key))
        if buckets:
            t0 = time.time()
            # ponytail: fixed 1000-cmd chunks; one giant pipeline here was a multi-minute
            # silent stall on big collections. Tune if a round trip ever dominates.
            for i in range(0, len(buckets), 1000):
                pipe = r.pipeline(transaction=False)
                for b_raw in buckets[i : i + 1000]:
                    b = b_raw.decode() if isinstance(b_raw, bytes) else b_raw
                    pipe.delete(b)
                pipe.execute()
            logging.info(
                f"[*] Cleared {len(buckets)} {level}:{field} index buckets in {time.time() - t0:.1f}s"
            )
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

        phase_t = time.time()

        def phase(msg):
            """Log a phase boundary with the elapsed time of the previous phase."""
            nonlocal phase_t
            now = time.time()
            logging.info(f"[*] sim-index: {msg} (prev phase {now - phase_t:.1f}s)")
            phase_t = now
            # This whole stage runs after the edge structures are freed, so it
            # looked cheap and was not measured. On the 11.3M-pair pool it is
            # where peak RSS actually lands, which is only visible with the
            # phase markers on.
            mem_util.phase(f"sim-index: {msg}", job_service, job_id)
            if job_service and job_id:
                job_service.add_log(job_id, msg)

        # Pool sim keys are namespaced under global:pool:{id} WITHOUT the algo segment,
        # and pool member fids are full source-collection fids ({srccoll}:func:{md5}:{addr}),
        # not algo-relative clean ids. Non-pool keys carry :{algo}: and strip to clean ids.
        is_pool = collection.startswith("global:pool:")
        sim_score_key = (
            f"{collection}:sim:score" if is_pool else f"{collection}:sim:score:{algo}"
        )

        from bsimvis.app.services.index_service import (
            _index_tag,
            _index_num,
            _unindex_tag,
            _unindex_num,
        )
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

        # NOTE: don't early-return when no cluster_* fields are configured to propagate.
        # The best-shared-cluster index ({col}:sim:best_cluster:{algo}) is always built
        # from cluster membership, independent of the legacy per-field propagation config.
        # Skipping the scan here left best_cluster empty → shared_clusters: [] in search.

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
            r.delete(f"{collection}:sim:best_cluster:{algo}")
            return True

        # BUILD PATH
        from bsimvis.app.services.config_service import config_service

        if not config_service.get("clustering.propagate_sim_indexes", True):
            phase(
                "Sim cluster index propagation disabled "
                "(clustering.propagate_sim_indexes=false) — skipping."
            )
            return True

        # 0. Wipe existing sim cluster indexes to avoid stale entries
        phase("Wiping stale sim cluster indexes...")
        for orig, target in cluster_prop:
            self._clear_indexes_via_registry(collection, "sim", target)
        for f in cluster_prop_num:
            r.delete(f"{collection}:idx:sim:{f}")
        r.delete(f"{collection}:sim:best_cluster:{algo}")

        # 1. Pre-fetch all cluster metadata records matching {collection}:cluster:{algo}:*:meta
        phase("Pre-fetching cluster metadata records...")

        cluster_meta_map = {}
        cluster_list_key = f"{collection}:cluster:list:{algo}"
        cids_raw = r.smembers(cluster_list_key)
        meta_keys = []
        if cids_raw:
            meta_keys = [
                f"{collection}:cluster:{algo}:{cid.decode() if isinstance(cid, bytes) else cid}:meta"
                for cid in cids_raw
            ]
        else:
            cursor = 0
            while True:
                cursor, keys = r.scan(
                    cursor=cursor,
                    match=f"{collection}:cluster:{algo}:*:meta",
                    count=1000,
                )
                meta_keys.extend(
                    [k.decode() if isinstance(k, bytes) else k for k in keys]
                )
                if cursor == 0:
                    break

        for i in range(0, len(meta_keys), 1000):
            chunk_keys = meta_keys[i : i + 1000]
            c_pipe = r.pipeline(transaction=False)
            for k in chunk_keys:
                c_pipe.get(k)
            res_list = c_pipe.execute()
            for k, res in zip(chunk_keys, res_list):
                if res:
                    cm = json.loads(res) if not isinstance(res, dict) else res
                    if isinstance(cm, str):
                        cm = json.loads(cm)
                    if cm and "cluster_id" in cm:
                        cid = str(cm["cluster_id"])
                        cluster_meta_map[cid] = cm

        # 2. Fetch function cluster metadata into memory (only for clustered functions)
        phase(
            f"Reading members of {len(cluster_meta_map)} clusters "
            "for similarity re-indexing..."
        )

        # First, gather all clustered function IDs by reading the members of all discovered clusters
        clustered_funcs_set = set()
        cid_list = list(cluster_meta_map.keys())
        for i in range(0, len(cid_list), 1000):
            m_pipe = r.pipeline(transaction=False)
            for cid in cid_list[i : i + 1000]:
                m_pipe.smembers(f"{collection}:cluster:{algo}:{cid}:members")
            for mem_set in m_pipe.execute():
                if mem_set:
                    for f_raw in mem_set:
                        clustered_funcs_set.add(
                            f_raw.decode() if isinstance(f_raw, bytes) else f_raw
                        )

        func_meta = {}
        funcs_list = list(clustered_funcs_set)
        phase(f"Fetching cluster assignments for {len(funcs_list)} functions...")

        for i in range(0, len(funcs_list), 1000):
            if i and i % 20000 == 0:
                logging.info(
                    f"[*] sim-index: cluster assignments {i}/{len(funcs_list)}"
                )
            chunk = funcs_list[i : i + 1000]
            pipe = r.pipeline(transaction=False)
            for fid_raw in chunk:
                fid = fid_raw.decode() if isinstance(fid_raw, bytes) else fid_raw
                if collection.startswith("global:pool:"):
                    pipe.smembers(f"{collection}:{fid}:clusters")
                    pipe.hgetall(f"{collection}:{fid}:cluster_scores")
                else:
                    pipe.smembers(f"{fid}:clusters")
                    pipe.hgetall(f"{fid}:cluster_scores")

            results = pipe.execute()

            for idx, fid_raw in enumerate(chunk):
                fid = fid_raw.decode() if isinstance(fid_raw, bytes) else fid_raw
                clusters_res = results[idx * 2]
                scores_res = results[idx * 2 + 1] or {}

                # Decode cluster IDs (strings)
                cluster_ids_str = (
                    [
                        c.decode() if isinstance(c, bytes) else str(c)
                        for c in clusters_res
                    ]
                    if clusters_res
                    else []
                )

                meta_entry = {}
                if cluster_ids_str:
                    cids = []
                    uuids = []
                    names = []
                    stabilities = []
                    for cid_str in cluster_ids_str:
                        cm = cluster_meta_map.get(cid_str)
                        if cm:
                            if cm.get("cluster_id") is not None:
                                cids.append(cm["cluster_id"])
                            if cm.get("cluster_uuid"):
                                uuids.append(cm["cluster_uuid"])
                            if cm.get("cluster_name"):
                                names.append(cm["cluster_name"])

                            score = 0.0
                            if isinstance(scores_res, dict):
                                for k, v in scores_res.items():
                                    k_str = k.decode() if isinstance(k, bytes) else k
                                    if k_str == cid_str:
                                        score = float(v)
                                        break
                            stabilities.append(
                                score or float(cm.get("cluster_stability", 0.0))
                            )

                    if cids:
                        meta_entry["cluster_id"] = cids
                    if uuids:
                        meta_entry["cluster_uuid"] = uuids
                    if names:
                        meta_entry["cluster_name"] = names
                    if stabilities:
                        meta_entry["cluster_stability"] = max(stabilities)

                func_meta[fid] = meta_entry

        # 3. Discover all similarities involving these clustered functions
        clustered_clean_ids = set()
        func_prefix = f"{collection}:func:"
        for fid, m in func_meta.items():
            if any(v is not None for v in m.values()):
                # Pool fids are already the involves-index key form; non-pool strip the prefix.
                clustered_clean_ids.add(fid if is_pool else fid[len(func_prefix) :])

        prefix = f"{collection}:sim:" if is_pool else f"{collection}:sim:{algo}:"
        clean_ids_list = list(clustered_clean_ids)
        total_funcs = len(clean_ids_list)
        total_sims = r.zcard(sim_score_key) or 0
        processed = 0
        indexed = 0

        phase(
            f"Propagating cluster indexes from {total_funcs} clustered functions "
            f"(out of {total_sims} total sims)..."
        )

        update_pipe = r.pipeline(transaction=False)

        start_prop = time.time()

        # Batch aggregators to compress Redis pipeline commands by over 99%
        tag_buckets = {}  # bucket_key -> set of sids
        reg_buckets = {}  # reg_key -> set of bucket_keys
        num_zsets = {}  # zset_key -> dict of sid: val
        # Forward map sid -> best shared cluster id, for display (search / bin_sim reads).
        best_cluster_key = f"{collection}:sim:best_cluster:{algo}"
        best_cluster_map = {}  # sid -> cluster_id

        def flush_batch():
            for b_key, sids in tag_buckets.items():
                if sids:
                    update_pipe.sadd(b_key, *sids)
            for r_key, b_keys in reg_buckets.items():
                if b_keys:
                    update_pipe.sadd(r_key, *b_keys)
            for z_key, mapping in num_zsets.items():
                if mapping:
                    update_pipe.zadd(z_key, mapping)
            if best_cluster_map:
                update_pipe.hset(best_cluster_key, mapping=best_cluster_map)
            update_pipe.execute()
            tag_buckets.clear()
            reg_buckets.clear()
            num_zsets.clear()
            best_cluster_map.clear()

        def iter_candidates():
            """Stream (sid, c1, c2) from the involves index, 1000 functions at a time.

            ponytail: deliberately no global candidate set. Materializing all sim ids
            (7M on a real collection) cost ~2GB and swapped the worker to a standstill.
            Each sim is emitted exactly once, by accepting it only from the involves
            set of its first endpoint — c1's set always contains it.
            """
            for i in range(0, total_funcs, 1000):
                if i and i % 50000 == 0:
                    logging.info(f"[*] sim-index: involves scan {i}/{total_funcs}")
                chunk = clean_ids_list[i : i + 1000]
                scan_pipe = r.pipeline(transaction=False)
                for c in chunk:
                    scan_pipe.smembers(f"{collection}:sim:involves:func:{c}")
                for c, res in zip(chunk, scan_pipe.execute()):
                    for sid_raw in res or ():
                        sid = (
                            sid_raw.decode() if isinstance(sid_raw, bytes) else sid_raw
                        )
                        if not sid.startswith(prefix):
                            continue
                        id_part = sid[len(prefix) :]
                        if "::" not in id_part:
                            continue
                        c1, c2 = id_part.split("::")
                        # Skip here when c is the second endpoint: c1's own set emits it.
                        if c1 == c:
                            yield sid, c1, c2

        for sid, c1, c2 in iter_candidates():
            # Skip if either function is not clustered
            if c1 not in clustered_clean_ids or c2 not in clustered_clean_ids:
                continue

            # Pool c1/c2 are already full source fids (func_meta keys); non-pool wrap them.
            fid1 = c1 if is_pool else f"{collection}:func:{c1}"
            fid2 = c2 if is_pool else f"{collection}:func:{c2}"
            m1 = func_meta.get(fid1, {})
            m2 = func_meta.get(fid2, {})

            # Pick the single best-matched shared cluster (highest cohesion). Indexing and
            # display both key off this one cluster, so an edge is only associated with the
            # cluster that actually best explains the match — not every cluster it touches.
            cids1 = [str(c) for c in (m1.get("cluster_id") or [])]
            cids2 = [str(c) for c in (m2.get("cluster_id") or [])]
            best = pick_best_shared_cluster(cids1, cids2, cluster_meta_map)
            if best is None:
                continue
            best_cid = str(best.get("cluster_id"))

            # Index TAG fields for the best cluster only.
            for orig, target in cluster_prop:
                v = best.get(orig)
                if v is None or v == "":
                    continue
                b_key = f"{collection}:idx:sim:{target}:{str(v).lower()}"
                r_key = f"{collection}:reg:sim:{target}"
                tag_buckets.setdefault(b_key, set()).add(sid)
                reg_buckets.setdefault(r_key, set()).add(b_key)

            # Index NUM fields for the best cluster only.
            for f in cluster_prop_num:
                v = best.get(f)
                if v is not None:
                    try:
                        z_key = f"{collection}:idx:sim:{f}"
                        num_zsets.setdefault(z_key, {})[sid] = float(v)
                    except (ValueError, TypeError):
                        pass

            best_cluster_map[sid] = best_cid
            indexed += 1
            processed += 1

            if processed % 5000 == 0:
                flush_batch()
                update_pipe = r.pipeline(transaction=False)
                logging.info(
                    f"[*] sim-index: propagated {processed}/~{total_sims} "
                    f"({time.time() - start_prop:.1f}s)"
                )
                if processed % 500_000 == 0:
                    mem_util.phase(
                        f"sim-index: propagated {processed}", job_service, job_id
                    )
                if job_service and job_id:
                    pct = (
                        min(int((processed / total_sims) * 100), 99)
                        if total_sims > 0
                        else 100
                    )
                    job_service.update_progress(
                        job_id,
                        pct,
                        f"Scanning similarities: {processed}/~{total_sims} ({indexed} indexed)",
                    )

        flush_batch()

        prop_time = time.time() - start_prop
        msg = f"Indexed {indexed} similarities with cluster info in {prop_time:.2f}s."
        logging.info(f"[+] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        return True

    def run_pool_clustering(
        self,
        pool_id,
        min_cluster_size=None,
        min_samples=None,
        cluster_selection_epsilon=None,
        selection_method=None,
        min_sim=None,
        min_features=None,
        job_service=None,
        job_id=None,
    ):
        """
        Runs HDBSCAN clustering on pool-namespaced similarity pairs by delegating to run_clustering.
        """
        from bsimvis.app.services.pool_service import pool_service
        from bsimvis.app.services.config_service import config_service

        pool = pool_service.get_pool(pool_id)
        if not pool:
            logging.error(f"Pool {pool_id} not found")
            return False

        func_cluster_params = pool.get("func_cluster_params", {})
        cluster_params = pool.get("cluster_params", {})

        if min_cluster_size is None:
            min_cluster_size = func_cluster_params.get("min_cluster_size")
            if min_cluster_size is None:
                min_cluster_size = cluster_params.get("min_cluster_size")
            if min_cluster_size is None:
                min_cluster_size = config_service.get("clustering.min_cluster_size", 2)
            min_cluster_size = int(min_cluster_size)

        if min_samples is None:
            min_samples = func_cluster_params.get("min_samples")
            if min_samples is None:
                min_samples = cluster_params.get("min_samples")
            if min_samples is None:
                min_samples = config_service.get("clustering.min_samples", 1)
            min_samples = int(min_samples)

        if cluster_selection_epsilon is None:
            cluster_selection_epsilon = func_cluster_params.get("epsilon")
            if cluster_selection_epsilon is None:
                cluster_selection_epsilon = cluster_params.get("epsilon")
            if cluster_selection_epsilon is None:
                cluster_selection_epsilon = config_service.get(
                    "clustering.epsilon", 0.1
                )
            cluster_selection_epsilon = float(cluster_selection_epsilon)

        if selection_method is None:
            selection_method = func_cluster_params.get("selection_method")
            if selection_method is None:
                selection_method = cluster_params.get("selection_method")
            if selection_method is None:
                selection_method = config_service.get(
                    "clustering.selection_method", "eom"
                )

        if min_sim is None:
            min_sim = func_cluster_params.get("min_sim")
            if min_sim is None:
                min_sim = cluster_params.get("min_sim")
            if min_sim is None:
                min_sim = config_service.get("clustering.min_sim", 0.0)
            min_sim = float(min_sim)

        if min_features is None:
            min_features = func_cluster_params.get("min_features")
            if min_features is None:
                min_features = cluster_params.get("min_features")
            if min_features is None:
                min_features = config_service.get("clustering.min_features", 0)
            min_features = int(min_features)

        algo = pool.get("algo", "unweighted_cosine")
        pool_coll = f"global:pool:{pool_id}"

        # Delegate to the robust run_clustering
        success = self.run_clustering(
            collection=pool_coll,
            algo=algo,
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            cluster_selection_epsilon=cluster_selection_epsilon,
            selection_method=selection_method,
            min_sim=min_sim,
            min_features=min_features,
            job_service=job_service,
            job_id=job_id,
        )
        if not success:
            return False

        self.r.hdel(f"global:pool:{pool_id}:meta", "total_func_clusters")
        return True

    def run_pool_bin_clustering(
        self,
        pool_id,
        min_cluster_size=None,
        min_samples=None,
        cluster_selection_epsilon=None,
        selection_method=None,
        min_sim=None,
        min_cohesion=None,
        job_service=None,
        job_id=None,
    ):
        """
        Runs HDBSCAN clustering on pool-namespaced binary similarity pairs.
        """
        from bsimvis.app.services.pool_service import pool_service
        from bsimvis.app.services.config_service import config_service

        pool = pool_service.get_pool(pool_id)
        if not pool:
            logging.error(f"Pool {pool_id} not found")
            return False

        # New structured config handling
        file_sim_params = pool.get("file_sim_params", {})
        if not file_sim_params.get("enabled", True):
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] File similarity disabled for pool {pool_id}, skipping run_pool_bin_clustering",
                )
            return True

        file_cluster_params = pool.get("file_cluster_params", {})
        cluster_params = pool.get("cluster_params", {})

        if min_cluster_size is None:
            min_cluster_size = file_cluster_params.get("min_cluster_size")
            if min_cluster_size is None:
                min_cluster_size = cluster_params.get("min_cluster_size")
            if min_cluster_size is None:
                min_cluster_size = config_service.get("clustering.min_cluster_size", 2)
            min_cluster_size = int(min_cluster_size)

        if min_samples is None:
            min_samples = file_cluster_params.get("min_samples")
            if min_samples is None:
                min_samples = cluster_params.get("min_samples")
            if min_samples is None:
                min_samples = config_service.get("clustering.min_samples", 1)
            min_samples = int(min_samples)

        if cluster_selection_epsilon is None:
            cluster_selection_epsilon = file_cluster_params.get("epsilon")
            if cluster_selection_epsilon is None:
                cluster_selection_epsilon = cluster_params.get("epsilon")
            if cluster_selection_epsilon is None:
                cluster_selection_epsilon = config_service.get(
                    "clustering.epsilon", 0.001
                )
            cluster_selection_epsilon = float(cluster_selection_epsilon)

        if selection_method is None:
            selection_method = file_cluster_params.get("selection_method")
            if selection_method is None:
                selection_method = cluster_params.get("selection_method")
            if selection_method is None:
                selection_method = config_service.get(
                    "clustering.selection_method", "eom"
                )

        if min_sim is None:
            min_sim = file_cluster_params.get("min_sim")
            if min_sim is None:
                min_sim = cluster_params.get("min_sim")
            if min_sim is None:
                min_sim = pool.get("min_score")
            if min_sim is None:
                min_sim = config_service.get("clustering.min_sim", 0.0)
            min_sim = float(min_sim)

        if hdbscan is None:
            logging.error("hdbscan library not installed.")
            return False

        import scipy.sparse as sp
        from scipy.sparse.csgraph import connected_components
        import pandas as pd

        r = self.r
        algo = pool.get("algo", "unweighted_cosine")
        sim_score_key = f"global:pool:{pool_id}:bin_sim:score:{algo}"
        prefix = f"global:pool:{pool_id}:bin_sim:{algo}:"

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Fetching pool binary similarity pairs from {sim_score_key}",
            )

        # Each half is "<coll>:<md5>", rebuilt as "<coll>:file:<md5>".
        def _pool_bin_ids(c1, c2):
            p1, p2 = c1.split(":"), c2.split(":")
            if len(p1) < 2 or len(p2) < 2:
                return None
            return f"{p1[0]}:file:{p1[1]}", f"{p2[0]}:file:{p2[1]}"

        # Streamed into typed arrays instead of a `pairs` list plus an `edges`
        # list. See sim_edges: that pattern cost 2.15 GiB on a real 5.4M-pair
        # set, against a 3 GB per-worker cap.
        edge_set = sim_edges.load_edges(
            r,
            sim_score_key,
            prefix,
            True,
            None,
            min_sim=min_sim,
            id_fn=_pool_bin_ids,
        )
        id_to_idx = edge_set.id_to_idx
        idx_to_id = edge_set.idx_to_id

        if edge_set.n_scanned == 0:
            logging.warning(f"No binary similarity pairs found for pool {pool_id}")
            return True

        if edge_set.src.size == 0:
            logging.warning(f"No valid edges for pool {pool_id} after filtering.")
            return True

        num_nodes = len(id_to_idx)

        # Split into connected components (same as BinClusterService.run_clustering)
        adj_matrix = sim_edges.build_adjacency(edge_set, num_nodes)
        n_components, comp_labels = connected_components(
            csgraph=adj_matrix, directed=False
        )

        comp_to_nodes = {}
        for i, comp_id in enumerate(comp_labels):
            comp_to_nodes.setdefault(comp_id, []).append(i)

        # Views into one sorted permutation, not a dict of tuple lists.
        comp_to_edges = sim_edges.group_edges_by_component(edge_set, comp_labels)

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] {n_components} connected components, running per-component HDBSCAN...",
            )

        global_tree_rows = []
        global_root_id = num_nodes
        next_cluster_id = num_nodes + 1
        comp_roots = []

        # Reused scratch: global index -> component-local index.
        gmap_pb = np.full(num_nodes, -1, dtype=np.int32)

        for comp_id, comp_nodes in comp_to_nodes.items():
            size = len(comp_nodes)
            if size < min_cluster_size:
                for node in comp_nodes:
                    comp_roots.append((node, 1))
                continue

            sub_id_to_global = {i: g for i, g in enumerate(comp_nodes)}

            # Unlike the function clustering paths, this one has no sparse
            # fallback for big components -- it always builds a dense size^2
            # float64 matrix. 0.8 GiB at 10k nodes, 3.2 GiB at 20k: an OOM kill
            # with no warning. Refuse instead, and say why.
            if size > CLUSTER_MAX_COMPONENT:
                msg = (
                    f"Pool binary component {comp_id} has {size} files; a dense "
                    f"distance matrix would need {size * size * 8 / 1024**3:.1f} GiB. "
                    f"Above CLUSTER_MAX_COMPONENT={CLUSTER_MAX_COMPONENT}, so its "
                    f"files are left unclustered rather than OOM-killing the worker."
                )
                logging.warning(f"[!] {msg}")
                if job_service and job_id:
                    job_service.add_log(job_id, msg)
                for node in comp_nodes:
                    comp_roots.append((node, 1))
                continue

            # float64 up front; see run_clustering above. Building float32 and
            # converting at fit time kept both matrices alive at once.
            sub_dist = np.ones((size, size), dtype=np.float64)
            np.fill_diagonal(sub_dist, 0)

            comp_nodes_arr = np.asarray(comp_nodes, dtype=np.int32)
            gmap_pb[comp_nodes_arr] = np.arange(size, dtype=np.int32)
            e_src, e_dst, e_dist = comp_to_edges.get(
                comp_id, (_EMPTY_I, _EMPTY_I, _EMPTY_F)
            )
            if e_src.size:
                ui = gmap_pb[e_src]
                vi = gmap_pb[e_dst]
                sub_dist[ui, vi] = e_dist
                sub_dist[vi, ui] = e_dist

            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=min(min_cluster_size, size),
                min_samples=min(min_samples, size),
                cluster_selection_epsilon=cluster_selection_epsilon,
                cluster_selection_method=selection_method,
                metric="precomputed",
                gen_min_span_tree=True,
            )
            clusterer.fit(sub_dist)

            local_tree_df = clusterer.condensed_tree_.to_pandas()
            if local_tree_df.empty:
                for node in comp_nodes:
                    comp_roots.append((node, 1))
                continue

            sub_internal_to_global = {}
            local_root_sub = local_tree_df["parent"].min()

            for row in local_tree_df.itertuples(index=False):
                parent = int(row.parent)
                child = int(row.child)
                if parent not in sub_internal_to_global:
                    sub_internal_to_global[parent] = next_cluster_id
                    next_cluster_id += 1
                if child < size:
                    global_child = sub_id_to_global[child]
                else:
                    if child not in sub_internal_to_global:
                        sub_internal_to_global[child] = next_cluster_id
                        next_cluster_id += 1
                    global_child = sub_internal_to_global[child]
                global_tree_rows.append(
                    {
                        "parent": sub_internal_to_global[parent],
                        "child": global_child,
                        "lambda_val": float(row.lambda_val),
                        "child_size": int(row.child_size),
                    }
                )

            comp_roots.append((sub_internal_to_global[local_root_sub], size))

        for comp_root, size in comp_roots:
            global_tree_rows.append(
                {
                    "parent": global_root_id,
                    "child": comp_root,
                    "lambda_val": 1.0,
                    "child_size": size,
                }
            )

        tree_df = pd.DataFrame(global_tree_rows)

        # Extract Condensed Tree for UI and Hierarchical Storage
        tree_json = tree_df.to_json(orient="records")
        tree_key = f"global:pool:{pool_id}:bin_cluster:tree:{algo}"
        r.set(tree_key, tree_json)

        tree_links = []
        for row in tree_df.itertuples(index=False):
            if int(row.child_size) > 1:
                tree_links.append(
                    {
                        "parent": int(row.parent),
                        "child": int(row.child),
                        "lambda": float(row.lambda_val),
                        "size": int(row.child_size),
                    }
                )
        r.set(
            f"global:pool:{pool_id}:bin_cluster:tree_links:{algo}",
            json.dumps(tree_links),
        )
        # Extract cluster members (shed noise excluded, synthetic root and
        # sub-min_cluster_size survivors dropped). See cluster_common.
        from bsimvis.app.services.cluster_common import hierarchical_membership

        leaf_to_clusters, _ = hierarchical_membership(
            tree_df, num_nodes, global_root_id, min_size=min_cluster_size
        )

        cluster_members = {}
        for leaf, clusters in leaf_to_clusters.items():
            for c in clusters:
                if c not in cluster_members:
                    cluster_members[c] = []
                fid = idx_to_id[leaf]
                if fid not in cluster_members[c]:
                    cluster_members[c].append(fid)

        root_id = tree_df["parent"].min()

        # Stability
        birth_lambdas = {root_id: 0.0}
        for row in tree_df.itertuples(index=False):
            if row.child_size > 1:
                birth_lambdas[int(row.child)] = float(row.lambda_val)

        leaf_death_lambdas = {}
        for row in tree_df.itertuples(index=False):
            if row.child_size == 1:
                leaf_death_lambdas[int(row.child)] = float(row.lambda_val)

        stabilities = {}
        for label, members in cluster_members.items():
            b_lambda = birth_lambdas.get(label, 0.0)
            total_area = sum(
                max(0.0, leaf_death_lambdas.get(id_to_idx[fid], b_lambda) - b_lambda)
                for fid in members
            )
            stabilities[label] = total_area

        # Sparse adjacency for cohesion. CSR rather than a dict of dicts -- see
        # sim_edges.SimAdjacency.
        adj_sim = sim_edges.SimAdjacency(edge_set, num_nodes)

        # Generate cluster UUIDs first
        label_to_uuid = {c: f"{uuid.uuid4().hex[:12]}" for c in cluster_members.keys()}

        # Bulk fetch all member metadata
        all_member_file_ids = []
        for members in cluster_members.values():
            all_member_file_ids.extend(members)
        all_member_file_ids = list(set(all_member_file_ids))

        all_member_meta = {}
        total_members = len(all_member_file_ids)
        for i in range(0, total_members, 1000):
            chunk = all_member_file_ids[i : i + 1000]
            m_pipe = r.pipeline(transaction=False)
            for fid in chunk:
                parts = fid.split(":")
                coll, md5 = parts[0], parts[2]
                m_pipe.get(f"{coll}:file:{md5}:meta")
            results = m_pipe.execute()
            for idx, fid in enumerate(chunk):
                meta_res = results[idx]
                m = {}
                if meta_res:
                    try:
                        m = json.loads(meta_res)
                    except Exception:
                        m = {}
                all_member_meta[fid] = m

        cluster_list_key = f"global:pool:{pool_id}:bin_cluster:list"
        pipe = r.pipeline(transaction=False)
        pipe.delete(cluster_list_key)

        for label, members in cluster_members.items():
            c_uuid = label_to_uuid[label]
            meta_key = f"global:pool:{pool_id}:bin_cluster:{c_uuid}:meta"
            members_key = f"global:pool:{pool_id}:bin_cluster:{c_uuid}:members"

            names_list = []
            md5s_list = []
            yara_list = []
            avtype_list = []
            filetype_list = []
            ccip_list = []

            for file_id in members:
                m = all_member_meta.get(file_id, {})
                if m.get("file_names"):
                    names_list.extend(m["file_names"])
                elif m.get("file_name"):
                    names_list.append(m["file_name"])

                if m.get("file_md5"):
                    md5s_list.append(m["file_md5"])

                if m.get("yara"):
                    yara_list.extend(
                        m["yara"] if isinstance(m["yara"], list) else [m["yara"]]
                    )
                if m.get("avtype"):
                    avtype_list.extend(
                        m["avtype"] if isinstance(m["avtype"], list) else [m["avtype"]]
                    )
                if m.get("filetype"):
                    filetype_list.extend(
                        m["filetype"]
                        if isinstance(m["filetype"], list)
                        else [m["filetype"]]
                    )
                if m.get("cc_ip"):
                    ccip_list.extend(
                        m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]]
                    )

            default_name = (
                Counter(names_list).most_common(1)[0][0]
                if names_list
                else f"Pool File Cluster {c_uuid}"
            )

            def build_freq(items):
                return (
                    [
                        {
                            "value": k,
                            "count": v,
                            "percent": round((v / len(members)) * 100),
                        }
                        for k, v in Counter(items).most_common(5)
                    ]
                    if items
                    else []
                )

            yara_freq = build_freq(yara_list)
            avtype_freq = build_freq(avtype_list)
            filetype_freq = build_freq(filetype_list)
            ccip_freq = build_freq(ccip_list)
            filename_freq = build_freq(names_list)
            md5_freq = build_freq(md5s_list)

            n_members = len(members)
            if n_members > 1:
                member_indices = [id_to_idx[fid] for fid in members]
                total_sim = adj_sim.cohesion_sum(member_indices)
                cohesion_score = total_sim / (n_members * (n_members - 1) / 2.0)
            else:
                cohesion_score = 1.0

            # Default min_cohesion is set before the function call
            min_cohesion_val = min_cohesion if min_cohesion is not None else 0.5
            if cohesion_score < min_cohesion_val:
                yara_freq = []
                avtype_freq = []
                filetype_freq = []
                ccip_freq = []
                filename_freq = []
                md5_freq = []

            rep_file_id = members[0] if members else None
            rep_meta = all_member_meta.get(rep_file_id, {}) if rep_file_id else {}
            snippet = rep_meta.get("file_name", "unknown")

            sample_members = []
            for member_fid in members[:5]:
                m = all_member_meta.get(member_fid, {})
                sample_members.append(
                    {
                        "id": member_fid,
                        "name": m.get("file_name", "Unknown"),
                        "file_name": m.get("file_name", "Unknown"),
                    }
                )

            meta = {
                "id": c_uuid,
                "cluster_uuid": c_uuid,
                "cluster_id": int(label),
                "name": default_name,
                "cluster_name": default_name,
                "snippet": snippet,
                "member_count": n_members,
                "cohesion_score": float(cohesion_score),
                "avg_stability": float(stabilities.get(label, 0.0)),
                "cluster_stability": float(stabilities.get(label, 0.0)),
                "created_at": int(time.time() * 1000),
                "sample_files": names_list[:5],
                "sample_members": sample_members,
                "yara_distribution": yara_freq,
                "avtype_distribution": avtype_freq,
                "filetype_distribution": filetype_freq,
                "ccip_distribution": ccip_freq,
                "filename_distribution": filename_freq,
                "md5_distribution": md5_freq,
            }

            for k, v in meta.items():
                if isinstance(v, float):
                    if not np.isfinite(v):
                        meta[k] = 0.0

            pipe.sadd(cluster_list_key, c_uuid)
            pipe.set(meta_key, json.dumps(meta))
            pipe.sadd(members_key, *members)

            # Secondary indexes for the pool
            collection_coll = f"global:pool:{pool_id}"

            # Index cluster_id
            bucket_key = (
                f"{collection_coll}:idx:file:bin_cluster_id:{str(label).lower()}"
            )
            pipe.sadd(bucket_key, *members)
            pipe.sadd(f"{collection_coll}:reg:file:bin_cluster_id", bucket_key)

            # Index cluster_uuid
            bucket_key = f"{collection_coll}:idx:file:bin_cluster_uuid:{c_uuid.lower()}"
            pipe.sadd(bucket_key, *members)
            pipe.sadd(f"{collection_coll}:reg:file:bin_cluster_uuid", bucket_key)

            # Index cluster_name
            bucket_key = (
                f"{collection_coll}:idx:file:bin_cluster_name:{default_name.lower()}"
            )
            pipe.sadd(bucket_key, *members)
            pipe.sadd(f"{collection_coll}:reg:file:bin_cluster_name", bucket_key)

            # Index top inferred metadata if cohesion is high enough
            if cohesion_score >= min_cohesion_val:
                inferred_mapping = {
                    "yara_distribution": "inferred_yara",
                    "avtype_distribution": "inferred_avtype",
                    "filetype_distribution": "inferred_filetype",
                    "ccip_distribution": "inferred_ccip",
                    "filename_distribution": "inferred_filename",
                    "md5_distribution": "inferred_md5",
                }
                for dist_key, meta_key in inferred_mapping.items():
                    dist = meta.get(dist_key) or []
                    if dist:
                        top_val = dist[0].get("value")
                        if top_val:
                            bucket_key = f"{collection_coll}:idx:file:{meta_key}:{str(top_val).lower()}"
                            pipe.sadd(bucket_key, *members)
                            pipe.sadd(
                                f"{collection_coll}:reg:file:{meta_key}", bucket_key
                            )

        pipe.execute()

        # Write file-to-cluster assignments: pool:{pool_id}:file:{md5}:bin_clusters
        pipe = r.pipeline(transaction=False)
        for i, (leaf, clusters) in enumerate(leaf_to_clusters.items()):
            file_id = idx_to_id[leaf]
            parts = file_id.split(":")
            if len(parts) >= 3:
                md5 = parts[2]
                clusters_key = f"pool:{pool_id}:file:{md5}:bin_clusters"
                if clusters:
                    cluster_uuids = [
                        label_to_uuid[c] for c in clusters if c in label_to_uuid
                    ]
                    if cluster_uuids:
                        pipe.delete(clusters_key)
                        pipe.sadd(clusters_key, *cluster_uuids)
                    else:
                        pipe.delete(clusters_key)
                else:
                    pipe.delete(clusters_key)
        pipe.execute()

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Pool binary clustering {pool_id} completed. Found {len(cluster_members)} clusters.",
            )

        self.r.hdel(f"global:pool:{pool_id}:meta", "total_file_clusters")
        return True


cluster_service = ClusterService()
