import struct
import logging

class GraphService:
    def __init__(self, redis_client):
        self.r = redis_client
        self.max_generations = 3
        self.partition_size = 4096

    def get_active_generation(self, collection):
        if not self.r:
            from bsimvis.app.services.redis_client import get_redis
            self.r = get_redis()
        gen = self.r.get(f"{collection}:graph:active_gen")
        if gen is None:
            return 0
        return int(gen)
        
    def set_active_generation(self, collection, gen):
        if not self.r:
            from bsimvis.app.services.redis_client import get_redis
            self.r = get_redis()
        self.r.set(f"{collection}:graph:active_gen", gen)

    def write_base_partitions(self, collection, gen, edges, part_offset=0):
        """Writes `edges` as BSC2 partitions starting at `part_offset`, so a
        caller streaming edges in batches (one call per batch, same `gen`)
        doesn't collide part indices -- each call must pass the offset
        returned by the previous one. Returns the next offset to use.
        """
        if not self.r:
            from bsimvis.app.services.redis_client import get_redis
            self.r = get_redis()
        if not edges:
            return part_offset

        partitions = [edges[i:i + self.partition_size] for i in range(0, len(edges), self.partition_size)]

        for i, part in enumerate(partitions):
            packed = [self.pack_bsc2_edge(e[0], e[1], e[2]) for e in part]
            self.r.set(f"{collection}:graph:gen:{gen}:part:{part_offset + i}", b"".join(packed))

        return part_offset + len(partitions)

    def pack_bsc2_edge(self, d_src, d_dst, score):
        scaled_score = int(score * 10000)
        return struct.pack("<IIH", d_src, d_dst, scaled_score)

    def unpack_bsc2_edge(self, binary_data):
        return struct.unpack("<IIH", binary_data)


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


    def store_edges(self, collection, edges):
        self.write_base_partitions(collection, self.get_active_generation(collection), edges)

graph_service = GraphService(None)
