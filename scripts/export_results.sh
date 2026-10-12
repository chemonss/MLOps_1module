#!/usr/bin/env bash
set -euo pipefail
mkdir -p results
for mode in baseline optimized; do
  file="sql/02_query_baseline.sql"
  if [[ "$mode" == "optimized" ]]; then file="sql/03_query_optimized.sql"; fi
  docker compose exec -T clickhouse clickhouse-client --user analyst --password analyst_password --format CSVWithNames --query "$(cat "$file")" > "results/${mode}.csv"
  echo "Exported results/${mode}.csv"
done
python3 scripts/check_results.py results/baseline.csv results/optimized.csv
