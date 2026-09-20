# Build, test and deploy the stack.
#
#     make            list the targets
#     make test       run the tests
#     make check      everything CI runs
#     make deploy     build and deploy, with AlertEmail on first run
#
# sam build builds the DuckDB layer itself, via layers/duckdb/Makefile,
# so there is no separate layer step to forget on a fresh clone.
#
# AWS credentials come from the ambient AWS_PROFILE, as in the other
# repositories:
#
#     AWS_PROFILE=<profile> make deploy ALERT_EMAIL=<address>

# Each recipe line runs under a strict shell, so a failing pipeline
# stops the target instead of the next line running against a
# half-built tree. Deliberately no .ONESHELL: macOS ships GNU Make
# 3.81, which predates it, so a recipe relying on it would work in CI
# and fall apart locally. Every recipe here is one command per line.
SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

REGION ?= ap-southeast-2
CONFIG_ENV ?= default
ALERT_EMAIL ?=

REQUIREMENTS := src/requirements.txt
LAYER_REQUIREMENTS := layers/duckdb/requirements.txt

.PHONY: help
help:  ## List the targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "} {printf "  %-12s %s\n", $$1, $$2}'

.PHONY: install
install:  ## Sync the virtualenv from the lockfile
	uv sync

.PHONY: test
test:  ## Run the tests
	uv run pytest tests/

.PHONY: lint
lint:  ## Run every linter CI runs
	uv run mypy src/ scripts/
	uv run isort --check-only src/ tests/ scripts/
	uv run pylint src/ scripts/
	uv run python -m scripts.check_layer_parity
	@# Guard the glob: numpydoc exits 0 given no files, which would
	@# silently retire the check.
	test "$$(find src scripts -name '*.py' | wc -l)" -gt 0 \
	  || { echo 'no python files found' >&2; exit 1; }
	find src scripts -name '*.py' -print0 | xargs -0 uv run numpydoc lint

.PHONY: check
check: test lint  ## Run the tests and every linter
	sam validate --lint --region=$(REGION)

.PHONY: build
build:  ## Export requirements and run sam build
	sam validate --region=$(REGION)
	uv export --no-dev --no-emit-project --no-color \
	  --format requirements-txt -o $(REQUIREMENTS)
	@# The layer's own pin, taken from the lockfile rather than written
	@# out anywhere. A literal would be a second place to update on
	@# every duckdb bump, and forgetting it would silently ship a layer
	@# whose Parquet and httpfs behaviour has drifted from what the
	@# tests ran against.
	uv export --all-groups --no-emit-project --no-hashes --no-color \
	  --format requirements-txt \
	  | grep '^duckdb==' > $(LAYER_REQUIREMENTS)
	sam build
	@# Removed once sam build has copied them into .aws-sam. Leaving
	@# them in the tree lets a stale export survive a lockfile change.
	rm -f $(REQUIREMENTS) $(LAYER_REQUIREMENTS)

.PHONY: deploy
deploy: build  ## Build and deploy the stack
	@# AlertEmail has no default in the template, so it must be supplied
	@# on a first deploy. The override is omitted when ALERT_EMAIL is
	@# empty, because passing an empty one would blank the stored value.
	sam deploy --config-env=$(CONFIG_ENV) --resolve-s3 \
	  --no-confirm-changeset --no-fail-on-empty-changeset \
	  $(if $(ALERT_EMAIL),--parameter-overrides AlertEmail=$(ALERT_EMAIL))

.PHONY: clean
clean:  ## Remove build artefacts
	rm -rf .aws-sam $(REQUIREMENTS) $(LAYER_REQUIREMENTS)
