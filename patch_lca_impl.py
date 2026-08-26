import re

with open('bsimvis/app/services/similarity_service.py', 'r') as f:
    content = f.read()

impl = """    def build_lca_snapshot(self, collection, algo="unweighted_cosine", workers=4):
        import bsimvis_similarity_native as sn
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
        
        # Use select_target_block for similarities
        indices = list(range(len(vectors)))
        # Assuming top_k=50, min_score=0.7
        edges_raw = scorer.select_target_block(indices, indices, algo, workers, 50, 0.7)
        
        # Store edges using delta-encoded class-ID pairs
        # stubbed out edge storing logic
        self._base_snapshot = edges_raw

"""
content = content.replace("    def _reset_read_caches(self):", impl + "    def _reset_read_caches(self):")

with open('bsimvis/app/services/similarity_service.py', 'w') as f:
    f.write(content)

