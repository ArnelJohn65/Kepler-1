#!/usr/bin/env bash
set -euo pipefail
python /app/data/generate_dataset.py /app/data
python /app/data/engine.py \
    /app/data/dataset.json \
    /app/data/queries.json \
    /app/results.json \
    /app/trace.jsonl
