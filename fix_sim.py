import re

with open('bsimvis/app/services/similarity_service.py', 'r') as f:
    content = f.read()

# 1. Update build_lca_snapshot
new_build = """    def build_lca_snapshot(self, collection, algo="unweighted_cosine", workers=4):
        import bsimvis_similarity_native as sn
        from bsimvis.app.services.config_service import config_service
        import logging

        r = self.r
        vclass_keys = r.keys(f"{collection}:vclass:*:vec:tf")
        if not vclass_keys:
            return
        
        vectors = []
        self.vclass_map = []
        pipe = r.pipeline(transaction=False)
        for key in vclass_keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            v_id = key_str.split(":")[2]
            self.vclass_map.append(v_id)
            pipe.zrange(key, 0, -1, withscores=True)
            
        results = pipe.execute()
        for vec in results:
            parsed = [(h.decode() if isinstance(h, bytes) else h, float(tf)) for h, tf in vec]
            vectors.append(parsed)
            
        scorer = sn.ExactScorer(vectors)
        indices = list(range(len(vectors)))
        
        backend = config_service.get("similarity.discovery_backend", "rust_cpu")
        min_score = config_service.get("similarity.min_score", 0.9)
        top_k = 0 # No top_k for discovery
        
        edges_raw = None
        if backend == "wgpu" and hasattr(scorer, "select_target_block_wgpu"):
            try:
                edges_raw = scorer.select_target_block_wgpu(indices, indices, algo, workers, top_k, min_score, 0.05)
                # Recompute and threshold in Rust f64 is done by the backend/we can also pass it to CPU scorer to be safe, but select_target_block_wgpu returns accurate scores.
            except Exception as e:
                logging.error(f"WGPU fallback on GPU failure with telemetry: {e}")
                edges_raw = None
                
        if edges_raw is None:
            edges_raw = scorer.select_target_block(indices, indices, algo, workers, top_k, min_score)
            
        self._base_snapshot = edges_raw"""

content = re.sub(r'    def build_lca_snapshot.*?(?=\n    def _reset_read_caches)', new_build, content, flags=re.DOTALL)

# 2. Remove top_k from discovery results slicing
content = re.sub(r'candidates = candidates\[:top_k\]\s*\n', '\n', content)

with open('bsimvis/app/services/similarity_service.py', 'w') as f:
    f.write(content)

