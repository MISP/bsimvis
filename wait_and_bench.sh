#!/bin/bash
echo "Waiting for bsimvis-bench to finish..."
while pgrep -f "bsimvis-bench" > /dev/null; do
    sleep 5
done
echo "Ingest finished. Running Dev full pipeline..."
uv run python scripts/benchmark_pipeline.py --base-url http://localhost:5001 --collection bench_dev_corpus --backend-label dev --out ../../data/bench_results/dev_corpus_full.json
echo "Running LCA full pipeline..."
uv run python scripts/benchmark_pipeline.py --base-url http://localhost:5460 --collection bench_lca_corpus --backend-label lca --out ../../data/bench_results/lca_corpus_full.json
echo "Done!"
