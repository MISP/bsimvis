import re

with open('bsimvis/app/services/feature_service.py', 'r') as f:
    content = f.read()

import_hashlib = "import hashlib\nimport os"
content = content.replace("import os", import_hashlib, 1)

lua_import = """from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.lua_manager import lua_manager"""
content = content.replace("from bsimvis.app.services.redis_client import get_redis", lua_import, 1)

write_buffer_init = """    def __init__(self):
        self.norms = {}  # norm key -> value
        self.zadds = defaultdict(dict)  # f_hash -> {v_id: tf}  (changed from func_id to v_id)
        self.incrs = defaultdict(float)  # f_hash -> summed tf
        self.metas = defaultdict(dict)  # f_hash -> {func_id: json}
        self.indexed = []  # func_ids
        
        # New for vclass
        self.vclass_funcs = defaultdict(list) # v_id -> [func_ids]
        self.vclass_norms = {} # v_id -> norm
"""
content = content.replace('    def __init__(self):\n        self.norms = {}  # norm key -> value\n        self.zadds = defaultdict(dict)  # f_hash -> {func_id: tf}\n        self.incrs = defaultdict(float)  # f_hash -> summed tf\n        self.metas = defaultdict(dict)  # f_hash -> {func_id: json}\n        self.indexed = []  # func_ids', write_buffer_init)

write_buffer_flush = """    def flush(self, pipe, collection):
        if self.norms:
            pipe.mset(self.norms)
        if self.vclass_norms:
            pipe.mset(self.vclass_norms)
        for f_hash, members in self.zadds.items():
            pipe.zadd(f"{collection}:feature:{f_hash}:vclasses", members)
        for f_hash, amount in self.incrs.items():
            pipe.zincrby(f"{collection}:features:by_tf", amount, f_hash)
        for f_hash, fields in self.metas.items():
            pipe.hset(f"{collection}:feature:{f_hash}:meta", mapping=fields)
        if self.indexed:
            pipe.sadd(f"{collection}:indexed:functions", *self.indexed)
        for v_id, funcs in self.vclass_funcs.items():
            pipe.sadd(f"{collection}:vclass:{v_id}:functions", *funcs)
        self.__init__()"""
content = content.replace('    def flush(self, pipe, collection):\n        if self.norms:\n            pipe.mset(self.norms)\n        for f_hash, members in self.zadds.items():\n            pipe.zadd(f"{collection}:feature:{f_hash}:functions", members)\n        for f_hash, amount in self.incrs.items():\n            pipe.zincrby(f"{collection}:features:by_tf", amount, f_hash)\n        for f_hash, fields in self.metas.items():\n            pipe.hset(f"{collection}:feature:{f_hash}:meta", mapping=fields)\n        if self.indexed:\n            pipe.sadd(f"{collection}:indexed:functions", *self.indexed)\n        self.__init__()', write_buffer_flush)


feature_service_init = """    def __init__(self, r=None):
        self.r = r or get_redis()
        self._vclass_script = lua_manager.get_script("get_or_create_vclass")"""
content = content.replace('    def __init__(self, r=None):\n        self.r = r or get_redis()', feature_service_init)


# Now patch the loop body
loop_body_old = """            # A. Recalculate L2 Norm
            sum_sq = sum(float(tf) ** 2 for _, tf in new_tf_data)
            acc.norms[f"{func_id}:vec:norm"] = math.sqrt(sum_sq)

            # B. Build Reverse Index (ZSETs)
            tf_dict = {
                h.decode() if isinstance(h, bytes) else str(h): float(score)
                for h, score in new_tf_data
            }

            for f_hash, new_tf in tf_dict.items():
                indexed_features.add(f_hash)
                # Update function mapping for this feature
                acc.zadds[f_hash][func_id] = new_tf
                # Update global TF counter for this feature
                acc.incrs[f_hash] += float(new_tf)"""
loop_body_new = """            # A. Recalculate L2 Norm
            sum_sq = sum(float(tf) ** 2 for _, tf in new_tf_data)
            vec_norm = math.sqrt(sum_sq)
            acc.norms[f"{func_id}:vec:norm"] = vec_norm

            # B. Get or Create Vector Class
            tf_dict = {
                h.decode() if isinstance(h, bytes) else str(h): float(score)
                for h, score in new_tf_data
            }
            
            # Sorted by hash for deterministic v_hash
            sorted_items = sorted(tf_dict.items(), key=lambda x: x[0])
            raw_str = ",".join(f"{k}:{v}" for k, v in sorted_items)
            v_hash = hashlib.sha256(raw_str.encode()).hexdigest()
            
            # Fetch from redis (wait, running lua script here synchronously could be slow, but it's okay)
            v_id_raw, created = self._vclass_script(keys=[v_hash], args=[collection], client=self.r)
            v_id = v_id_raw.decode() if isinstance(v_id_raw, bytes) else str(v_id_raw)
            
            # Map function to this v_id
            acc.vclass_funcs[v_id].append(func_id)
            
            if int(created) == 1:
                # First time seeing this vector class, so we index it!
                acc.vclass_norms[f"{collection}:vclass:{v_id}:norm"] = vec_norm
                
                for f_hash, new_tf in tf_dict.items():
                    indexed_features.add(f_hash)
                    # Update vclass mapping for this feature
                    acc.zadds[f_hash][v_id] = new_tf
                    # Update global TF counter for this feature
                    acc.incrs[f_hash] += float(new_tf)"""
content = content.replace(loop_body_old, loop_body_new)

with open('bsimvis/app/services/feature_service.py', 'w') as f:
    f.write(content)
