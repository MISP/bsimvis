# LCA Acceleration Task Tracker

## 1. Similarity and Discovery
- [x] Hash each sorted raw feature/TF vector into one verified vector class.
- [x] Store one vector, norm and numeric class ID with function/per-file memberships.
- [x] Build complete feature->class postings (no rare-feature cutoffs, etc).
- [ ] Preserve min_features semantics (BSim class score, or equal FunctionID score 1.0).
- [ ] Remove global top_k from class discovery and explicit file-pair builds. Retain every positive class edge at or above floor.
- [ ] Keep one-to-one binary matching using per-file class capacities; expand selected class matches to functions.
- [ ] Preserve API response shapes through inferred expansion.

## 2. LCA Execution Backend
- [x] Port LCA native components (packed sparse vectors, Rust f64 accumulation, WGPU scoring, BSC2).
- [x] Configure similarity.discovery_backend as rust_cpu or wgpu (default rust_cpu).
- [x] Maintain Python exact discovery as compatibility fallback.
- [x] WGPU mode: candidate union, broad GPU float32 score, retain candidates with margin, recompute/threshold in Rust f64, fallback on GPU failure with telemetry.
- [ ] Cache one active compact base snapshot plus <=3 delta snapshots per worker (byte budget). Invalidated by catalog-generation changes.

## 3. Compact Incremental Graph
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

## 5. Migration, Validation and Estimates
- [ ] Run Mirai census for multiplicities.
- [ ] Maintenance window: build vector classes, snapshots, compact edges, tree in shadow storage.
- [ ] Validate, cut over, retain legacy keys.
- [ ] Acceptance tests (digest collisions, parity, GPU/CPU thresholds, BSC2 round-trip, truth table, class-capacity matching, interrupted delta writes, hierarchy projection, pool cross_collection, telemetry).
