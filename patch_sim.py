import re
with open('bsimvis/app/services/similarity_service.py', 'r') as f:
    content = f.read()
old_code = """        for k in vclass_keys:
            k_str = k.decode() if isinstance(k, bytes) else k
            v_id = int(k_str.split(":")[2])
            vclass_funcs[v_id] = [f.decode() if isinstance(f, bytes) else f for f in r.smembers(k)]"""
new_code = """        for k in vclass_keys:
            k_str = k.decode() if isinstance(k, bytes) else k
            try:
                v_id = int(k_str.split(":")[2])
            except ValueError:
                continue
            vclass_funcs[v_id] = [f.decode() if isinstance(f, bytes) else f for f in r.smembers(k)]"""
content = content.replace(old_code, new_code)
with open('bsimvis/app/services/similarity_service.py', 'w') as f:
    f.write(content)
