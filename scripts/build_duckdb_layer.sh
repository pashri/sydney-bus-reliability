#!/usr/bin/env bash
# Build the DuckDB Lambda layer for the merger.
#
# duckdb is dev-only in pyproject.toml: a runtime dependency is bundled
# into EVERY function's package, which pushed the schedule loader over
# Lambda's 250 MB unzipped limit. Only the merger needs duckdb, so it
# gets it from this layer instead.
set -euo pipefail
OUT="${1:-layers/duckdb}"
rm -rf "$OUT" && mkdir -p "$OUT/python"
# Pinned to the version uv.lock resolved, rather than written literally
# here: a literal would be a second place to update on every duckdb
# bump, and forgetting it would silently ship a layer whose Parquet
# and httpfs behaviour has drifted from what the tests ran against.
VERSION="$(uv run python -c 'import duckdb; print(duckdb.__version__)')"
echo "Building DuckDB layer for duckdb==${VERSION}"
pip download --platform manylinux_2_28_aarch64 --python-version 3.14 \
  --only-binary=:all: --no-deps -d "$OUT/.wheels" "duckdb==${VERSION}"
python -m zipfile -e "$OUT"/.wheels/duckdb-*.whl "$OUT/python"
rm -rf "$OUT/.wheels"
# httpfs is not in the wheel, unlike icu, json and parquet, which are
# statically linked into it. Downloading it at runtime would make every
# cold start depend on DuckDB's extension repository being reachable,
# and the merger cold-starts on every scheduled run. Baked in here, a
# version or platform mismatch fails the deploy instead.
EXTENSIONS="$OUT/python/duckdb_extensions"
mkdir -p "$EXTENSIONS"
curl -fsSL \
  "http://extensions.duckdb.org/v${VERSION}/linux_arm64/httpfs.duckdb_extension.gz" \
  | gunzip > "$EXTENSIONS/httpfs.duckdb_extension"
du -sh "$OUT/python"
