#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="$PROJECT_ROOT/output/dashboard"
TARGET_DIR="$PROJECT_ROOT/frontend/public/data"

mkdir -p "$TARGET_DIR"
cp "$SOURCE_DIR"/*.csv "$TARGET_DIR"/

echo "Frontend dashboard data refreshed in $TARGET_DIR"
