#!/bin/bash
echo "Waiting for Dev ingest..."
wait $1
echo "Waiting for LCA ingest..."
wait $2

echo "Running Full Pipeline on Dev..."
uv run python scripts/benchmark_pipeline.py --base-url http://localhost:5001 --collection bench_dev_corpus --backend-label dev --out ../../data/bench_results/dev_corpus_full.json

echo "Running Full Pipeline on LCA..."
uv run python scripts/benchmark_pipeline.py --base-url http://localhost:5460 --collection bench_lca_corpus --backend-label lca --out ../../data/bench_results/lca_corpus_full.json
