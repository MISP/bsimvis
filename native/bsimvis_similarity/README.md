# BSIMVis Native Similarity

Optional Rust CPU scoring primitives for the experimental branch. Python owns algorithms, parameters, scheduling policy, and persistence. This crate receives immutable sparse vectors and bounded pair work; it does not access Kvrocks or Redis.

Build into the project environment:

```bash
uv run maturin develop --release \
  --manifest-path native/bsimvis_similarity/Cargo.toml
```

Run parity tests:

```bash
uv run python -m unittest \
  tests.test_similarity_backends \
  tests.test_similarity_compatibility

cargo test --release \
  --manifest-path native/bsimvis_similarity/Cargo.toml
```

The extension is optional. `python_exact` remains the default backend unless `rust_cpu` is selected explicitly.
