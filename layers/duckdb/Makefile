# Filename for this makefile case sensitive when being run in a pipeline.
# It must be "Makefile".
# Use tabs, not spaces, for indentation in this file or the build may fail.
# SAM runs this target itself, because the layer is declared with
# `BuildMethod: makefile`, so there is no separate build step to forget.
#
# The duckdb version comes from `requirements.txt`, which deploy.sh
# exports from uv.lock before calling sam build. SAM copies
# this directory elsewhere before running make, so the repository is
# not reachable from here and the version cannot be looked up live.

build-DuckdbLayer:

	@# duckdb is dev-only in pyproject.toml: a runtime dependency is
	@# bundled into EVERY function's package, which pushed the schedule
	@# loader over Lambda's 250 MB unzipped limit. Only the merger and
	@# the checker need duckdb, so they get it from this layer instead.
	@mkdir -p "$(ARTIFACTS_DIR)/python" > /dev/null

	$(eval VERSION := $(shell sed -n 's/^duckdb==\([^ ;]*\).*/\1/p' requirements.txt))
	@# Fail loudly rather than building a layer for "duckdb==", which
	@# pip reports as an invalid requirement several lines later.
	@test -n "$(VERSION)" \
	  || { echo 'no duckdb pin in requirements.txt' >&2; exit 1; }
	@echo "Building DuckDB layer for duckdb==$(VERSION)"

	@pip download --platform manylinux_2_28_aarch64 --python-version 3.14 \
	  --only-binary=:all: --no-deps -d "$(ARTIFACTS_DIR)/.wheels" \
	  "duckdb==$(VERSION)" > /dev/null
	@python -m zipfile -e "$(ARTIFACTS_DIR)"/.wheels/duckdb-*.whl \
	  "$(ARTIFACTS_DIR)/python" > /dev/null
	@rm -rf "$(ARTIFACTS_DIR)/.wheels" > /dev/null

	@# httpfs is not in the wheel, unlike icu, json and parquet, which
	@# are statically linked into it. Downloading it at runtime would
	@# make every cold start depend on DuckDB's extension repository
	@# being reachable, and the merger cold-starts on every scheduled
	@# run. Baked in here, a version or platform mismatch fails the
	@# build instead.
	@#
	@# aws as well as httpfs: loading httpfs from the layer turns
	@# autoloading off, and CREATE SECRET ... PROVIDER credential_chain
	@# lives in aws, so without it every merge fails before reading a
	@# row.
	@mkdir -p "$(ARTIFACTS_DIR)/python/duckdb_extensions" > /dev/null
	@for ext in httpfs aws; do \
	  curl -fsSL \
	    "http://extensions.duckdb.org/v$(VERSION)/linux_arm64/$${ext}.duckdb_extension.gz" \
	    | gunzip \
	    > "$(ARTIFACTS_DIR)/python/duckdb_extensions/$${ext}.duckdb_extension"; \
	done

	@du -sh "$(ARTIFACTS_DIR)/python"
