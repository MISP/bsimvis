with open('bsimvis/app/services/processing_service.py', 'r') as f:
    content = f.read()

import re
content = re.sub(r'                # Vector Class hashing\n                import hashlib\n                sorted_items = sorted\(zset_mapping\.items\(\), key=lambda x: x\[0\]\)\n                raw_str = ","\.join\(f"\{k\}:\{v\}" for k, v in sorted_items\)\n                vec_hash = hashlib\.sha256\(raw_str\.encode\(\)\)\.hexdigest\(\)\n                \n                pipe\.sadd\(f"\{collection\}:vclass:\{vec_hash\}:functions", base_func_key\)\n                pipe\.hset\(f"\{collection\}:vclass_map", base_func_key, vec_hash\)\n', '', content)

with open('bsimvis/app/services/processing_service.py', 'w') as f:
    f.write(content)
