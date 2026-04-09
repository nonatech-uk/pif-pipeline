#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Copy shared UI library into build context (excluded from git)
rm -rf mees-shared-ui
cp -r ../mees-shared-ui .
rm -rf mees-shared-ui/.git mees-shared-ui/node_modules

podman build -t pipeline:latest .

# Clean up
rm -rf mees-shared-ui
