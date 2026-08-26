import re

with open('bsimvis/app/services/processing_service.py', 'r') as f:
    content = f.read()

replacement = """            vec_tf_list = func_features.get("bsim_features_tf", [])
            if vec_tf_list:
                zset_mapping = {item["hash"]: item["tf"] for item in vec_tf_list}
                pipe.zadd(f"{base_func_key}:vec:tf", zset_mapping)
                
                # Vector Class hashing
                import hashlib
                sorted_items = sorted(zset_mapping.items(), key=lambda x: x[0])
                raw_str = ",".join(f"{k}:{v}" for k, v in sorted_items)
                vec_hash = hashlib.sha256(raw_str.encode()).hexdigest()
                
                pipe.sadd(f"{collection}:vclass:{vec_hash}:functions", base_func_key)
                pipe.hset(f"{collection}:vclass_map", base_func_key, vec_hash)
"""

content = content.replace('            vec_tf_list = func_features.get("bsim_features_tf", [])\n            if vec_tf_list:\n                zset_mapping = {item["hash"]: item["tf"] for item in vec_tf_list}\n                pipe.zadd(f"{base_func_key}:vec:tf", zset_mapping)\n', replacement)

with open('bsimvis/app/services/processing_service.py', 'w') as f:
    f.write(content)
