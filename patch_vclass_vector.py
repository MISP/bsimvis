import re

with open('bsimvis/app/services/feature_service.py', 'r') as f:
    content = f.read()

write_buffer_init_old = """        # New for vclass
        self.vclass_funcs = defaultdict(list) # v_id -> [func_ids]
        self.vclass_norms = {} # v_id -> norm"""
write_buffer_init_new = """        # New for vclass
        self.vclass_funcs = defaultdict(list) # v_id -> [func_ids]
        self.vclass_norms = {} # v_id -> norm
        self.vclass_tfs = defaultdict(dict) # v_id -> {f_hash: tf}"""
content = content.replace(write_buffer_init_old, write_buffer_init_new)

flush_old = """        for v_id, funcs in self.vclass_funcs.items():
            pipe.sadd(f"{collection}:vclass:{v_id}:functions", *funcs)
        self.__init__()"""
flush_new = """        for v_id, funcs in self.vclass_funcs.items():
            pipe.sadd(f"{collection}:vclass:{v_id}:functions", *funcs)
        for v_id, tf_map in self.vclass_tfs.items():
            pipe.zadd(f"{collection}:vclass:{v_id}:vec:tf", tf_map)
        self.__init__()"""
content = content.replace(flush_old, flush_new)

loop_body_old = """                for f_hash, new_tf in tf_dict.items():
                    indexed_features.add(f_hash)
                    # Update vclass mapping for this feature
                    acc.zadds[f_hash][v_id] = new_tf
                    # Update global TF counter for this feature
                    acc.incrs[f_hash] += float(new_tf)"""
loop_body_new = """                acc.vclass_tfs[v_id] = tf_dict
                for f_hash, new_tf in tf_dict.items():
                    indexed_features.add(f_hash)
                    # Update vclass mapping for this feature
                    acc.zadds[f_hash][v_id] = new_tf
                    # Update global TF counter for this feature
                    acc.incrs[f_hash] += float(new_tf)"""
content = content.replace(loop_body_old, loop_body_new)

with open('bsimvis/app/services/feature_service.py', 'w') as f:
    f.write(content)
