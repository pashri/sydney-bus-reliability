#!/usr/bin/env bash

# This script should make it easy to deploy an AWS stack via SAM.
#
# Deploy (default):
#     AWS_PROFILE=<profile> ./deploy.sh
#
# Deploy to a named samconfig environment:
#     AWS_PROFILE=<profile> ./deploy.sh -c development
#
# Build only (no deploy):
#     AWS_PROFILE=<profile> ./deploy.sh --no-deploy
#
# AlertEmail has no default in the template, so a first deploy against a
# new stack needs it. Later deploys reuse the stored value.
#     AWS_PROFILE=<profile> ALERT_EMAIL=<address> ./deploy.sh

# Fail if anything exits with error code
set -euo pipefail
IFS=$'\n\t'
shopt -s failglob

# Help function
helpfunction() {
    echo ""
    echo "Usage: $0 [-c CONFIG_ENV] [-n|--no-deploy]"
    echo -e "\t-c (optional) Config environment"
    echo -e "\t-n, --no-deploy (optional) Validate and build only"
    exit 1
}

# Parse arguments
CONFIG_ENV="default"
NO_DEPLOY=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c) CONFIG_ENV="$2"; shift 2 ;;
        -n|--no-deploy) NO_DEPLOY=true; shift ;;
        *) helpfunction ;;
    esac
done

# Set other parameters
export REGION="ap-southeast-2"  # This is just for validation. Deployment will use the region in the SAM config file, which is also ap-southeast-2.
export ALERT_EMAIL="${ALERT_EMAIL:-}"

# Validate, export dependencies, build, remove exports, and deploy
echo "Validate, export dependencies, build, and deploy"
sam validate --region="${REGION}"
uv export --no-dev --no-emit-project --no-color \
  --format requirements-txt -o src/requirements.txt

# Create the DuckDB layer using the version from pyproject.toml
uv export --all-groups --no-emit-project --no-hashes --no-color \
  --format requirements-txt \
  | grep '^duckdb==' > layers/duckdb/requirements.txt

sam build
rm src/requirements.txt layers/duckdb/requirements.txt

# Exit early if --no-deploy flag is set
if [ "$NO_DEPLOY" = true ]; then
    echo "Build complete. Skipping deployment (--no-deploy flag set)."
    exit 0
fi

# Only override AlertEmail when one was given. Passing an empty override
# would blank the value already stored on the stack.
if [ -n "$ALERT_EMAIL" ]; then
    sam deploy --config-env="${CONFIG_ENV}" --resolve-s3 \
      --no-confirm-changeset --no-fail-on-empty-changeset \
      --parameter-overrides "AlertEmail=${ALERT_EMAIL}"
else
    sam deploy --config-env="${CONFIG_ENV}" --resolve-s3 \
      --no-confirm-changeset --no-fail-on-empty-changeset
fi

# If there are any post-deployment steps, they can be added below.
