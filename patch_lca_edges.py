import re

with open('bsimvis/app/services/similarity_service.py', 'r') as f:
    content = f.read()

replacement = """        mapped_edges = []
        # vclass_map gives stable ids, edges_raw uses indices 0...len(vectors)-1
        for u, targets in enumerate(edges_raw):
            for v, s in targets:
                mapped_edges.append((int(self.vclass_map[u]), int(self.vclass_map[v]), s))"""

content = re.sub(r'        mapped_edges = \[\]\n        # vclass_map gives stable ids, edges_raw uses indices 0\.\.\.len\(vectors\)-1\n        for u, v, s in edges_raw:\n            mapped_edges\.append\(\(int\(self\.vclass_map\[u\]\), int\(self\.vclass_map\[v\]\), s\)\)', replacement, content)

with open('bsimvis/app/services/similarity_service.py', 'w') as f:
    f.write(content)
