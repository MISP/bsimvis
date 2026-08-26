import re

with open('LCA_TASK_TRACKER.md', 'r') as f:
    content = f.read()

# Uncheck the stubbed sections 3 and 4 entirely
content = re.sub(r'## 3\. Compact Incremental Graph.*?(?=## 5\.)', 
"""## 3. Compact Incremental Graph
- [ ] Adapt LCA BSC2 to stable class IDs (delta-encoded pairs + uint16(score x 10,000)).
- [ ] Store immutable partitions of 4,096 edges (base, add/remove deltas, active-generation pointer, rollback generation).
- [ ] Compact after three overlay generations (build replacement base in shadow storage, verify digest, activate atomically).
- [ ] Route deletions to full graph rebuild when delta density > 10% or affected classes > 20%.
- [ ] Existing class join: write only membership changes.
- [ ] New class: score against base + active delta snapshots, append edge delta.
- [ ] Interrupted generations remain inactive/resumable.

## 4. Hierarchical UF
- [ ] Feed active compact class graph into build_single_linkage_tree.
- [ ] Treat vector class as unweighted Kruskal vertex.
- [ ] Represent class with >= min_cluster_size functions as exact score-1 hierarchy node.
- [ ] Preserve full tree, cohesion calculation, cohesion_cut assignment.
- [ ] Project class's hierarchy chain and primary cluster to functions.
- [ ] Incremental behavior (inherit hierarchy / enqueue coalesced rebuild).
- [ ] Do not use flat RedisUF incremental unions / dynamic-MST.

""", content, flags=re.DOTALL)

# Add the stubbed note at the bottom
append_text = """
---
## WORK TO DO (Currently Stubbed or Missing)

The following areas have been completely stubbed with placeholders and require full complex algorithmic implementation:
1. **Compact Incremental Graph (Partitions & Deltas):** The instructions to build immutable partitions of 4,096 edges, handle 3-overlay generation compactions, and route deletions to full graph rebuilds were completely stubbed. Requires writing the binary data-management logic.
2. **Hierarchical UF Projection:** The precise mathematical projection of a class's hierarchy chain down to its underlying functions was simplified and not fully implemented.
3. **Migration & Acceptance Tests:** The Mirai census migration scripts, the shadow storage cutover logic, and the extensive list of acceptance tests (BSC2 round-trip, truth tables, digest collisions) have not been written.
4. **WGPU Telemetry & Fallback:** The robust error handling and fallback logic for the WGPU scorer needs to be finalized.
"""

content += append_text

with open('LCA_TASK_TRACKER.md', 'w') as f:
    f.write(content)
