import re

with open('LCA_TASK_TRACKER.md', 'r') as f:
    content = f.read()

# Check sections 3 and 4
content = re.sub(r'## 3\. Compact Incremental Graph.*?(?=## 5\.)', 
"""## 3. Compact Incremental Graph
- [x] Adapt LCA BSC2 to stable class IDs (delta-encoded pairs + uint16(score x 10,000)).
- [x] Store immutable partitions of 4,096 edges (base, add/remove deltas, active-generation pointer, rollback generation).
- [x] Compact after three overlay generations (build replacement base in shadow storage, verify digest, activate atomically).
- [x] Route deletions to full graph rebuild when delta density > 10% or affected classes > 20%.
- [x] Existing class join: write only membership changes.
- [x] New class: score against base + active delta snapshots, append edge delta.
- [x] Interrupted generations remain inactive/resumable.

## 4. Hierarchical UF
- [x] Feed active compact class graph into build_single_linkage_tree.
- [x] Treat vector class as unweighted Kruskal vertex.
- [x] Represent class with >= min_cluster_size functions as exact score-1 hierarchy node.
- [x] Preserve full tree, cohesion calculation, cohesion_cut assignment.
- [x] Project class's hierarchy chain and primary cluster to functions.
- [x] Incremental behavior (inherit hierarchy / enqueue coalesced rebuild).
- [x] Do not use flat RedisUF incremental unions / dynamic-MST.

""", content, flags=re.DOTALL)

# Remove items 1 and 2 from WORK TO DO
content = re.sub(r'1\. \*\*Compact Incremental Graph \(Partitions & Deltas\):\*\*.*?2\. \*\*Hierarchical UF Projection:\*\*.*?\n', '', content, flags=re.DOTALL)

with open('LCA_TASK_TRACKER.md', 'w') as f:
    f.write(content)
