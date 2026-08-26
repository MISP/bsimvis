with open("bsimvis/app/services/graph_service.py", "r") as f:
    content = f.read()

new_methods = """
    def get_edges_for_gen(self, collection, gen):
        if not self.r:
            from bsimvis.app.services.redis_client import get_redis
            self.r = get_redis()
            
        edges = []
        idx = 0
        while True:
            packed_data = self.r.get(f"{collection}:graph:gen:{gen}:part:{idx}")
            if not packed_data:
                break
            
            # unpack BSC2 edges
            edge_size = struct.calcsize("<IIH")
            for i in range(0, len(packed_data), edge_size):
                chunk = packed_data[i:i+edge_size]
                if len(chunk) == edge_size:
                    u, v, scaled_score = self.unpack_bsc2_edge(chunk)
                    edges.append((u, v, scaled_score / 10000.0))
            idx += 1
            
        return edges

    def compact_partitions(self, collection):
        logging.info(f"Compacting graph partitions for {collection}...")
        if not self.r:
            from bsimvis.app.services.redis_client import get_redis
            self.r = get_redis()
            
        # keep last 3 generations, delete older
        active_gen = self.get_active_generation(collection)
        oldest_keep = max(0, active_gen - self.max_generations)
        
        for g in range(0, oldest_keep):
            idx = 0
            while True:
                key = f"{collection}:graph:gen:{g}:part:{idx}"
                if not self.r.exists(key):
                    break
                self.r.delete(key)
                idx += 1
"""

content = content.replace("    def compact_partitions(self, collection):\n        logging.info(f\"Compacting graph partitions for {collection}...\")\n        pass", new_methods)

with open("bsimvis/app/services/graph_service.py", "w") as f:
    f.write(content)
