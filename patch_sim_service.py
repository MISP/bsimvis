import re

with open('bsimvis/app/services/similarity_service.py', 'r') as f:
    content = f.read()

snapshot_cache = """        self._pl_budget = 5_000_000
        self._norm_cache = {}  # v_id -> vector norm (float)
        self._count_cache = {}  # (count_idx_key, v_id) -> feature count (float)
        
        # LCA Acceleration Graph Cache
        self._base_snapshot = None
        self._delta_snapshots = []
        self._snapshot_budget_bytes = 1024 * 1024 * 500 # 500MB
"""
content = content.replace('        self._pl_budget = 5_000_000\n        self._norm_cache = {}  # func_id -> vector norm (float)\n        self._count_cache = {}  # (count_idx_key, func_id) -> feature count (float)', snapshot_cache)

with open('bsimvis/app/services/similarity_service.py', 'w') as f:
    f.write(content)
