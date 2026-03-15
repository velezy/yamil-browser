#!/bin/bash
# =============================================================================
# AI Builder Orchestra Service — One-Time Setup
# Bundles the shared library so the service is fully self-contained.
#
# Usage:
#   ./setup.sh                    # if inside AssemblyLine repo
#   ./setup.sh /path/to/shared    # if copied elsewhere
#   docker compose up             # after setup
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_NAME="$(basename "$SCRIPT_DIR")"
TARGET_DIR="$SCRIPT_DIR/assemblyline-common"

# Check if already bundled
if [ -d "$TARGET_DIR/assemblyline_common" ]; then
    echo "✓ assemblyline-common already bundled in $SERVICE_NAME. Run 'docker compose up'."
    exit 0
fi

# Try to find the shared library
SHARED_DIR="${1:-}"

if [ -z "$SHARED_DIR" ]; then
    CANDIDATES=(
        "$SCRIPT_DIR/../../shared/python"
        "$SCRIPT_DIR/../../../shared/python"
        "$SCRIPT_DIR/../shared/python"
    )
    for candidate in "${CANDIDATES[@]}"; do
        if [ -f "$candidate/pyproject.toml" ] && [ -d "$candidate/assemblyline_common" ]; then
            SHARED_DIR="$candidate"
            break
        fi
    done
fi

if [ -z "$SHARED_DIR" ] || [ ! -d "$SHARED_DIR/assemblyline_common" ]; then
    echo "ERROR: Could not find assemblyline-common shared library."
    echo ""
    echo "Options:"
    echo "  1. Run from inside the AssemblyLine repo:  ./setup.sh"
    echo "  2. Specify path manually:                  ./setup.sh /path/to/shared/python"
    echo "  3. Copy shared/python/ to ./assemblyline-common/ manually"
    echo ""
    echo "The shared library is at: AssemblyLine/shared/python/"
    exit 1
fi

echo "[$SERVICE_NAME] Bundling assemblyline-common from: $SHARED_DIR"
mkdir -p "$TARGET_DIR"
cp -r "$SHARED_DIR/assemblyline_common" "$TARGET_DIR/"
cp "$SHARED_DIR/pyproject.toml" "$TARGET_DIR/"
[ -f "$SHARED_DIR/README.md" ] && cp "$SHARED_DIR/README.md" "$TARGET_DIR/" || true

echo ""
echo "✓ $SERVICE_NAME: assemblyline-common bundled successfully."
echo "✓ Run 'docker compose up' to start."
