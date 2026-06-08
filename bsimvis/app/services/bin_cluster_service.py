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


class BinClusterService:
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
            cluster_selection_epsilon = config_service.get("clustering.epsilon", 0.001)
        if selection_method is None:
            selection_method = config_service.get("clustering.selection_method", "eom")
        if min_sim is None:
            min_sim = config_service.get("clustering.min_sim", 0.0)

        if hdbscan is None:
            logging.error(
                "hdbscan library not installed. Please install it to use clustering."
            )
            return False

        r = self.r
        sim_score_key = f"{collection}:bin_sim:score:{algo}"

        # 1. Fetch all similarity pairs
        logging.info(f"[*] Fetching binary similarity pairs from {sim_score_key}...")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Fetching binary similarity pairs for {collection} ({algo})..."
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
            if len(pairs) % 5000 == 0:
                logging.info(f"[*] Fetched {len(pairs)} binary similarity pairs...")

        msg = f"Fetched {len(pairs)} binary similarity pairs."
        logging.info(f"[+] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        if not pairs:
            logging.warning(f"No binary similarity pairs found for {collection}:{algo}")
            return True

        prefix = f"{collection}:bin_sim:{algo}:"

        # 2. Build identity mapping and edge list
        # We need a numeric mapping for HDBSCAN
        id_to_idx = {}
        idx_to_id = {}
        edges = []

        for sid, score in pairs:
            # sid format: {coll}:bin_sim:{algo}:{md5_a}::{md5_b}
            if not sid.startswith(prefix):
                continue

            ids_part = sid[len(prefix) :]
            if "::" not in ids_part:
                continue

            m1, m2 = ids_part.split("::")
            file_id1 = f"{collection}:file:{m1}"
            file_id2 = f"{collection}:file:{m2}"

            for fid in [file_id1, file_id2]:
                if fid not in id_to_idx:
                    idx = len(id_to_idx)
                    id_to_idx[fid] = idx
                    idx_to_id[idx] = fid

            # 2.5 Apply similarity threshold if provided
            score_val = float(score)
            if min_sim > 0 and score_val < min_sim:
                continue

            # HDBSCAN works with distance. Distance = 1 - score
            dist = max(0, 1.0 - score_val)
            edges.append((id_to_idx[file_id1], id_to_idx[file_id2], dist))

        if not edges:
            logging.warning(
                f"No valid edges found for {collection}:{algo} after parsing {len(pairs)} binary pairs."
            )
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"Error: No valid similarity edges found after parsing {len(pairs)} binary pairs. Check filters.",
                )
            return True

        num_nodes = len(id_to_idx)
        msg = f"Building binary graph with {num_nodes} files and {len(edges)} similarity edges..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        import scipy.sparse as sp
        from scipy.sparse.csgraph import connected_components
        import pandas as pd

        msg = f"Shattering binary graph into connected components to avoid OOM..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        rows = []
        cols = []
        data = []
        for i, j, d in edges:
            if d < 1.0: # Only real similarity edges
                rows.extend([i, j])
                cols.extend([j, i])
                data.extend([1, 1])

        adj_matrix = sp.csr_matrix((data, (rows, cols)), shape=(num_nodes, num_nodes))
        n_components, labels = connected_components(csgraph=adj_matrix, directed=False)
        
        comp_to_nodes = {}
        for i, comp_id in enumerate(labels):
            if comp_id not in comp_to_nodes:
                comp_to_nodes[comp_id] = []
            comp_to_nodes[comp_id].append(i)

        comp_to_edges = {}
        for i, j, d in edges:
            c = labels[i]
            if c == labels[j]:
                if c not in comp_to_edges:
                    comp_to_edges[c] = []
                comp_to_edges[c].append((i, j, d))

        msg = f"Found {n_components} connected components. Running local HDBSCAN..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        global_tree_rows = []
        global_root_id = num_nodes
        next_cluster_id = num_nodes + 1
        comp_roots = []
        
        start_fit = time.time()
        
        for comp_id, comp_nodes in comp_to_nodes.items():
            size = len(comp_nodes)
            if size < min_cluster_size:
                for node in comp_nodes:
                    comp_roots.append((node, 1))
                continue
                
            sub_id_to_global = {i: global_idx for i, global_idx in enumerate(comp_nodes)}
            global_to_sub_id = {global_idx: i for i, global_idx in enumerate(comp_nodes)}
            
            if size >= 5000:
                from scipy.sparse.linalg import svds
                
                rows_sp, cols_sp, data_sp = [], [], []
                if comp_id in comp_to_edges:
                    for u, v, d in comp_to_edges[comp_id]:
                        ui = global_to_sub_id[u]
                        vi = global_to_sub_id[v]
                        sim = 1.0 - d
                        rows_sp.extend([ui, vi])
                        cols_sp.extend([vi, ui])
                        data_sp.extend([sim, sim])
                
                comp_matrix = sp.csr_matrix((data_sp, (rows_sp, cols_sp)), shape=(size, size), dtype=np.float32)
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
                sub_dist = np.ones((size, size), dtype=np.float32)
                np.fill_diagonal(sub_dist, 0)
                
                if comp_id in comp_to_edges:
                    for u, v, d in comp_to_edges[comp_id]:
                        ui = global_to_sub_id[u]
                        vi = global_to_sub_id[v]
                        sub_dist[ui, vi] = d
                        sub_dist[vi, ui] = d
                        
                clusterer = hdbscan.HDBSCAN(
                    min_cluster_size=min(min_cluster_size, size),
                    min_samples=min(min_samples, size),
                    cluster_selection_epsilon=cluster_selection_epsilon,
                    cluster_selection_method=selection_method,
                    metric="precomputed",
                    gen_min_span_tree=True,
                )
                clusterer.fit(sub_dist.astype(np.float64))
            
            local_tree_df = clusterer.condensed_tree_.to_pandas()
            if local_tree_df.empty:
                for node in comp_nodes:
                    comp_roots.append((node, 1))
                continue
                
            sub_internal_to_global = {}
            # Ensure local root maps to a single global internal ID
            local_root_sub = local_tree_df['parent'].min()
            
            for _, row in local_tree_df.iterrows():
                parent = int(row['parent'])
                child = int(row['child'])
                
                if parent not in sub_internal_to_global:
                    sub_internal_to_global[parent] = next_cluster_id
                    next_cluster_id += 1
                    
                if child < size: # Leaf
                    global_child = sub_id_to_global[child]
                else: # Internal
                    if child not in sub_internal_to_global:
                        sub_internal_to_global[child] = next_cluster_id
                        next_cluster_id += 1
                    global_child = sub_internal_to_global[child]
                    
                global_tree_rows.append({
                    'parent': sub_internal_to_global[parent],
                    'child': global_child,
                    'lambda_val': float(row['lambda_val']),
                    'child_size': int(row['child_size'])
                })
                
            comp_roots.append((sub_internal_to_global[local_root_sub], size))
            
        # Stitch all roots to a synthetic global root at lambda 1.0 (distance 1.0)
        for comp_root, size in comp_roots:
            global_tree_rows.append({
                'parent': global_root_id,
                'child': comp_root,
                'lambda_val': 1.0,
                'child_size': size
            })
            
        tree_df = pd.DataFrame(global_tree_rows)
        fit_time = time.time() - start_fit
        
        msg = f"HDBSCAN fit completed in {fit_time:.2f}s."
        logging.info(f"[+] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        msg = f"Global condensed tree has {len(tree_df)} rows."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # 1. Birth lambdas for all clusters
        root_id = tree_df["parent"].min()
        birth_lambdas = {root_id: 0.0}
        for _, row in tree_df.iterrows():
            if row["child_size"] > 1:
                birth_lambdas[int(row["child"])] = float(row["lambda_val"])

        # 2. Death lambdas for all clusters
        death_lambdas = {}
        for _, row in tree_df.iterrows():
            p = int(row["parent"])
            l = float(row["lambda_val"])
            if p not in death_lambdas or l > death_lambdas[p]:
                death_lambdas[p] = l

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
            for _, row in tree_df.iterrows():
                parent = int(row["parent"])
                child = int(row["child"])
                child_size = int(row["child_size"])
                lambda_val = float(row["lambda_val"])

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

        # 4. Extract Condensed Tree for UI
        tree_json = tree_df.to_json(orient="records")
        tree_key = f"{collection}:bin_cluster:tree:{algo}"
        r.set(tree_key, tree_json)

        cluster_tree_key = f"{collection}:bin_cluster:tree_links:{algo}"
        tree_links = []
        for _, row in tree_df.iterrows():
            if int(row["child_size"]) > 1:
                tree_links.append(
                    {
                        "parent": int(row["parent"]),
                        "child": int(row["child"]),
                        "lambda": float(row["lambda_val"]),
                        "size": int(row["child_size"]),
                    }
                )
        r.set(cluster_tree_key, json.dumps(tree_links))

        logging.info("[*] Extracting hierarchical clusters from tree...")
        if job_service and job_id:
            job_service.add_log(job_id, "Extracting hierarchical clusters from tree...")

        child_to_parent = dict(zip(tree_df["child"], tree_df["parent"]))

        leaf_to_clusters = {}
        for leaf in range(num_nodes):
            clusters = set()
            curr = leaf
            while curr in child_to_parent:
                p = child_to_parent[curr]
                clusters.add(int(p))
                curr = p
            leaf_to_clusters[leaf] = list(clusters)

        cluster_members = {}
        for leaf, clusters in leaf_to_clusters.items():
            for c in clusters:
                if c not in cluster_members:
                    cluster_members[c] = []
                cluster_members[c].append(idx_to_id[leaf])

        label_to_uuid = {c: f"{uuid.uuid4().hex[:12]}" for c in cluster_members.keys()}

        # 5. Calculate Stability
        stabilities = {}
        leaf_death_lambdas = {}
        for _, row in tree_df.iterrows():
            if row["child_size"] == 1:
                leaf_death_lambdas[int(row["child"])] = float(row["lambda_val"])

        for label, members in cluster_members.items():
            b_lambda = birth_lambdas.get(label, 0.0)
            total_area = 0.0
            for file_id in members:
                leaf_idx = id_to_idx[file_id]
                d_lambda = leaf_death_lambdas.get(leaf_idx, b_lambda)
                total_area += max(0.0, d_lambda - b_lambda)
            stabilities[label] = total_area

        # 5. Persist binary assignments
        logging.info(f"[*] Persisting {len(cluster_members)} binary clusters...")
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Persisting {len(cluster_members)} binary clusters and assigning files...",
            )

        from bsimvis.app.services.index_service import _index_tag, _unindex_tag

        pipe = r.pipeline()

        for c, members in cluster_members.items():
            pipe.sadd(f"{collection}:bin_cluster:{algo}:{c}:members", *members)
            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        # Extract and save direct members (where child_size == 1)
        direct_members = {}
        for _, row in tree_df.iterrows():
            if int(row["child_size"]) == 1:
                p = int(row["parent"])
                leaf = int(row["child"])
                if leaf in idx_to_id:
                    fid = idx_to_id[leaf]
                    if p not in direct_members:
                        direct_members[p] = []
                    direct_members[p].append(fid)

        for c, d_members in direct_members.items():
            pipe.sadd(f"{collection}:bin_cluster:{algo}:{c}:direct_members", *d_members)
            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        file_tag_fields = [
            f
            for f in self.get_native_fields("file", False)
            if f.startswith("bin_cluster_")
        ]

        for i, (leaf, clusters) in enumerate(leaf_to_clusters.items()):
            file_id = idx_to_id[leaf]
            clusters_key = f"{file_id}:bin_clusters"
            scores_key = f"{file_id}:bin_cluster_scores"

            if clusters:
                pipe.delete(clusters_key)
                pipe.sadd(clusters_key, *clusters)

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

            if i % 500 == 0:
                pipe.execute()
                if job_service and job_id:
                    pct = int((i / num_nodes) * 50)
                    job_service.update_progress(job_id, pct)

        pipe.execute()

        # Update secondary index for 'bin_cluster_id', 'bin_cluster_uuid', 'bin_cluster_name'
        logging.info(
            f"[*] Updating secondary indexes for {len(cluster_members)} binary clusters..."
        )
        for idx, (label, members) in enumerate(cluster_members.items()):
            if "bin_cluster_id" in file_tag_fields:
                bucket_key = (
                    f"{collection}:idx:file:bin_cluster_id:{str(label).lower()}"
                )
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:file:bin_cluster_id", bucket_key)

            if "bin_cluster_uuid" in file_tag_fields:
                c_uuid = label_to_uuid[label]
                bucket_key = f"{collection}:idx:file:bin_cluster_uuid:{c_uuid.lower()}"
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:file:bin_cluster_uuid", bucket_key)

            if idx % 100 == 0:
                pipe.execute()
        pipe.execute()

        # 6. Calculate Cluster Metadata
        logging.info(
            f"[*] Calculating enriched metadata for {len(cluster_members)} binary clusters..."
        )

        all_member_file_ids = list(id_to_idx.keys())
        all_member_meta = {}
        total_members = len(all_member_file_ids)
        msg = f"Pre-fetching metadata for {total_members} files..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        for i in range(0, total_members, 1000):
            chunk = all_member_file_ids[i : i + 1000]
            m_pipe = r.pipeline()
            for file_id in chunk:
                m_pipe.json().get(f"{file_id}:meta", "$")
            results = m_pipe.execute()
            for idx, file_id in enumerate(chunk):
                meta_res = results[idx]
                m = meta_res[0] if isinstance(meta_res, list) and meta_res else {}
                if isinstance(m, str):
                    m = json.loads(m)
                all_member_meta[file_id] = m
            
            if i % 1000 == 0:
                logging.info(f"[*] Fetched meta for {i}/{total_members} files...")

        # Build sparse adjacency dictionary of similarities for fast cohesion calculation
        msg = "Building sparse adjacency map for cohesion calculation..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)
            
        adj_sim = {i: {} for i in range(num_nodes)}
        for u, v, d in edges:
            sim = 1.0 - d
            adj_sim[u][v] = sim
            adj_sim[v][u] = sim

        total_clusters = len(cluster_members)
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Enriching metadata for {total_clusters} hierarchical binary clusters...",
            )

        for idx, (label, members) in enumerate(cluster_members.items()):
            names = []
            yara_list = []
            avtype_list = []
            filetype_list = []
            ccip_list = []

            for file_id in members:
                m = all_member_meta.get(file_id, {})
                if m.get("file_name"):
                    names.append(m["file_name"])
                if m.get("yara"):
                    yara_list.extend(m["yara"] if isinstance(m["yara"], list) else [m["yara"]])
                if m.get("avtype"):
                    avtype_list.extend(m["avtype"] if isinstance(m["avtype"], list) else [m["avtype"]])
                if m.get("filetype"):
                    filetype_list.extend(m["filetype"] if isinstance(m["filetype"], list) else [m["filetype"]])
                if m.get("cc_ip"):
                    ccip_list.extend(m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]])

            default_name = (
                Counter(names).most_common(1)[0][0]
                if names
                else f"Binary Cluster {label}"
            )
            
            def build_freq(items):
                return [{"value": k, "count": v, "percent": round((v / len(items)) * 100)} for k, v in Counter(items).most_common(5)] if items else []

            yara_freq = build_freq(yara_list)
            avtype_freq = build_freq(avtype_list)
            filetype_freq = build_freq(filetype_list)
            ccip_freq = build_freq(ccip_list)

            # Exact Average Internal Similarity (Cohesion) using sparse adjacency map
            if len(members) > 1:
                member_indices = [id_to_idx[file_id] for file_id in members]
                n_members = len(members)

                total_sim = 0.0
                if n_members < 50:
                    for i in range(n_members):
                        u = member_indices[i]
                        for j in range(i + 1, n_members):
                            v = member_indices[j]
                            total_sim += adj_sim[u].get(v, 0.0)
                else:
                    member_set = set(member_indices)
                    for u in member_indices:
                        for v, sim in adj_sim[u].items():
                            if v in member_set:
                                total_sim += sim
                    total_sim /= 2.0

                cohesion_score = total_sim / (n_members * (n_members - 1) / 2.0)
            else:
                cohesion_score = 1.0

            rep_file_id = members[0] if members else None
            rep_meta = all_member_meta.get(rep_file_id, {}) if rep_file_id else {}
            snippet = rep_meta.get("file_name", "unknown")

            meta = {
                "cluster_id": int(label),
                "snippet": snippet,
                "cluster_uuid": label_to_uuid[label],
                "cluster_name": default_name,
                "cohesion_score": float(cohesion_score),
                "avg_stability": float(stabilities.get(label, 0.0)),
                "cluster_stability": float(stabilities.get(label, 0.0)),
                "member_count": len(members),
                "sample_members": names[:5],
                "yara_distribution": yara_freq,
                "avtype_distribution": avtype_freq,
                "filetype_distribution": filetype_freq,
                "ccip_distribution": ccip_freq,
                "created_at": int(time.time() * 1000),
            }

            for k, v in meta.items():
                if isinstance(v, float):
                    if not np.isfinite(v):
                        meta[k] = 0.0
            pipe.json().set(f"{collection}:bin_cluster:{algo}:{label}:meta", "$", meta)

            if "bin_cluster_name" in file_tag_fields:
                bucket_key = (
                    f"{collection}:idx:file:bin_cluster_name:{default_name.lower()}"
                )
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:file:bin_cluster_name", bucket_key)

            if job_service and job_id and (idx + 1) % 50 == 0:
                pct = 50 + int(((idx + 1) / total_clusters) * 50)
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Enriching binary clusters: {idx + 1}/{total_clusters}",
                )

        pipe.execute()

        cluster_list_key = f"{collection}:bin_cluster:list:{algo}"
        r.delete(cluster_list_key)
        if cluster_members:
            r.sadd(cluster_list_key, *[str(k) for k in cluster_members.keys()])

        summary = f"Binary clustering complete. Found {len(cluster_members)} hierarchical clusters."
        logging.info(f"[+] {summary}")
        if job_service and job_id:
            job_service.add_log(job_id, summary)

        return True

    def clear_clusters(
        self, collection, algo="unweighted_cosine", job_service=None, job_id=None
    ):
        """
        Clears all binary clustering data for a collection and algorithm.
        """
        r = self.r

        cluster_list_key = f"{collection}:bin_cluster:list:{algo}"
        cids_raw = r.smembers(cluster_list_key)
        all_meta_keys = []
        if cids_raw:
            all_meta_keys = [
                f"{collection}:bin_cluster:{algo}:{cid.decode() if isinstance(cid, bytes) else cid}:meta"
                for cid in cids_raw
            ]
        else:
            pattern = f"{collection}:bin_cluster:{algo}:*:meta"
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                all_meta_keys.extend(
                    [k.decode() if isinstance(k, bytes) else k for k in keys]
                )
                if cursor == 0:
                    break

        cluster_ids = []
        prefix = f"{collection}:bin_cluster:{algo}:"
        for k in all_meta_keys:
            cid = k[len(prefix) : -len(":meta")]
            cluster_ids.append(cid)

        total_clusters = len(cluster_ids)
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Cleaning up binary clustering data for {total_clusters} clusters...",
            )

        from bsimvis.app.services.index_service import _unindex_tag

        for i, cid in enumerate(cluster_ids):
            members_key = f"{collection}:bin_cluster:{algo}:{cid}:members"
            members = r.smembers(members_key)
            if members:
                pipe = r.pipeline()
                for j, mid_raw in enumerate(members):
                    mid = mid_raw.decode() if isinstance(mid_raw, bytes) else mid_raw
                    _unindex_tag(pipe, collection, "file", "bin_cluster_id", cid, mid)
                    pipe.delete(f"{mid}:bin_clusters")
                    pipe.delete(f"{mid}:bin_cluster_scores")

                    if j % 500 == 0:
                        pipe.execute()
                pipe.execute()

            r.delete(f"{collection}:bin_cluster:{algo}:{cid}:members")
            r.delete(f"{collection}:bin_cluster:{algo}:{cid}:direct_members")
            r.delete(f"{collection}:bin_cluster:{algo}:{cid}:meta")

            if job_service and job_id and i % 10 == 0:
                pct = int((i / total_clusters) * 100)
                job_service.update_progress(job_id, pct)

        # Clear named-based indexes
        self._clear_indexes_via_registry(collection, "file", "bin_cluster_name")
        self._clear_indexes_via_registry(collection, "file", "bin_cluster_uuid")
        self._clear_indexes_via_registry(collection, "file", "bin_cluster_id")

        r.delete(f"{collection}:bin_cluster:tree:{algo}")
        r.delete(f"{collection}:bin_cluster:list:{algo}")
        r.delete(f"{collection}:bin_cluster:tree_links:{algo}")

        if job_service and job_id:
            job_service.add_log(job_id, "Binary clustering data cleared successfully.")
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


bin_cluster_service = BinClusterService()
