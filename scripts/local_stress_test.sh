set -euo pipefail

# ---- args (edit here) ----
UV_RUNNER=(uv run)
CMD=(benchbox run)

SCALE="1"
COMPRESSION="zstd:3"

PLATFORMS=(
  duckdb
  polars
  datafusion
  datafusion-df
)

BENCHMARKS=(
  tpch
  tpcds
  tpcdi
  ssb
  h2odb
  amplab
  joinorder
  clickbench
  coffeeshop
  tsbs_devops
  nyctaxi
  tpchavoc
  tpch_skew
  tpcds_obt
  datavault
  read_primitives
  write_primitives
  metadata_primitives
  transaction_primitives
)

# ---- run ----
for platform in "${PLATFORMS[@]}"; do
  for benchmark in "${BENCHMARKS[@]}"; do
    "${UV_RUNNER[@]}" "${CMD[@]}" \
      --platform "$platform" \
      --benchmark "$benchmark" \
      --scale "$SCALE" \
      --compression "$COMPRESSION"
  done
done
