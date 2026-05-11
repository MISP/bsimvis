import logging
import json
import time
import numpy as np
from bsimvis.app.services.redis_client import get_redis

try:
    import hdbscan
except ImportError:
    hdbscan = None

class ClusterService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def run_clustering(self, collection, algo="unweighted_cosine", min_cluster_size=5, job_service=None, job_id=None):
        """
        Runs HDBSCAN clustering on similarity pairs stored in Kvrocks.
        """
        if hdbscan is None:
            logging.error("hdbscan library not installed. Please install it to use clustering.")
            return False

        r = self.r
        sim_score_key = f"{collection}:sim:score:{algo}"
        
        # 1. Fetch all similarity pairs
        logging.info(f"[*] Fetching similarity pairs from {sim_score_key}...")
        if job_service and job_id:
            job_service.add_log(job_id, f"Fetching similarity pairs for {collection} ({algo})...")

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
            
            ids_part = sid[len(prefix):]
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
            
            # HDBSCAN works with distance. Distance = 1 - score (for normalized cosine)
            dist = max(0, 1.0 - float(score))
            edges.append((id_to_idx[fid1], id_to_idx[fid2], dist))

        if not edges:
            logging.warning(f"No valid edges found for {collection}:{algo} after parsing {len(pairs)} pairs.")
            if job_service and job_id:
                job_service.add_log(job_id, f"Error: No valid similarity edges found after parsing {len(pairs)} pairs. Check key format.")
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
            metric='precomputed',
            gen_min_span_tree=True
        )
        cluster_labels = clusterer.fit_predict(dist_matrix.astype(np.float64))
        
        # 4. Extract Condensed Tree for UI
        tree = clusterer.condensed_tree_.to_pandas()
        tree_json = tree.to_json(orient='records')
        tree_key = f"{collection}:cluster:tree:{algo}"
        r.set(tree_key, tree_json)

        # 5. Persist Results
        logging.info(f"[*] Persisting cluster results for {num_nodes} functions...")
        if job_service and job_id:
            job_service.add_log(job_id, "Persisting cluster results to Kvrocks...")

        from bsimvis.app.services.index_service import _index_tag, _index_num

        pipe = r.pipeline()
        cluster_counts = {}
        
        for idx, label in enumerate(cluster_labels):
            fid = idx_to_id[idx]
            label = int(label)
            stability = float(clusterer.probabilities_[idx])
            
            # Update function metadata
            meta_key = f"{fid}:meta"
            
            if label != -1: # -1 is noise in HDBSCAN
                cluster_str = f"cluster_{label}"
                r.json().set(meta_key, "$.cluster_id", cluster_str)
                r.json().set(meta_key, "$.cluster_stability", stability)
                
                # Update Secondary Indexes
                _index_tag(pipe, collection, "func", "cluster_id", cluster_str, fid)
                _index_num(pipe, collection, "func", "cluster_stability", stability, fid)
                
                # Add to cluster membership set
                cluster_set_key = f"{collection}:cluster:{algo}:{cluster_str}:members"
                pipe.sadd(cluster_set_key, fid)
                
                # Track counts for logging
                cluster_counts[cluster_str] = cluster_counts.get(cluster_str, 0) + 1
            else:
                r.json().set(meta_key, "$.cluster_id", "noise")
                r.json().set(meta_key, "$.cluster_stability", 0.0)
                _index_tag(pipe, collection, "func", "cluster_id", "noise", fid)
                _index_num(pipe, collection, "func", "cluster_stability", 0.0, fid)

            # Periodically execute pipeline
            if idx % 100 == 0:
                pipe.execute()
                if job_service and job_id:
                    pct = int((idx / num_nodes) * 100)
                    job_service.update_progress(job_id, pct)

        pipe.execute()
        
        summary = f"Clustering complete. Found {len(cluster_counts)} clusters. Noise: {list(cluster_labels).count(-1)} functions."
        logging.info(f"[+] {summary}")
        if job_service and job_id:
            job_service.add_log(job_id, summary)

        return True

cluster_service = ClusterService()
