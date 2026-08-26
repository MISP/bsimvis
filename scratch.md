You need to implement the Python side of the Unique-Vector Similarity with LCA Acceleration plan.
Wait for `rust_porter` to finish exposing the `bsimvis_similarity` native module, or stub it out for now.
Read the plan from the user prompt:
- Hash each sorted raw feature/TF vector into one verified vector class.
- Store one vector, norm and numeric class ID with function/per-file memberships.
- Replace per-function discovery with unique raw-vector classes.
- Feed the active compact class graph into `build_single_linkage_tree` and `hierarchical_membership`.
- Represent a class with at least `min_cluster_size` functions as an exact score-1 hierarchy node.
