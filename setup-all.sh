#!/bin/bash
# =============================================================================
# YAMIL Browser — One-Time Setup
# Bundles assemblyline-common into chat-service and rag-service so they are
# fully self-contained Docker images.
#
# Usage:
#   ./setup-all.sh                                    # auto-detect from AssemblyLine repo
#   ./setup-all.sh /path/to/AssemblyLine/shared/python # explicit path
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHARED_DIR="${1:-}"

# Try to find the shared library
if [ -z "$SHARED_DIR" ]; then
    CANDIDATES=(
        "$SCRIPT_DIR/../Yamil/AssemblyLine/shared/python"
        "$SCRIPT_DIR/../../Yamil/AssemblyLine/shared/python"
        "$HOME/Project/Git/Yamil/AssemblyLine/shared/python"
    )
    for candidate in "${CANDIDATES[@]}"; do
        if [ -f "$candidate/pyproject.toml" ] && [ -d "$candidate/assemblyline_common" ]; then
            SHARED_DIR="$candidate"
            echo "Found assemblyline-common at: $SHARED_DIR"
            break
        fi
    done
fi

if [ -z "$SHARED_DIR" ] || [ ! -d "$SHARED_DIR/assemblyline_common" ]; then
    echo "ERROR: Could not find assemblyline-common shared library."
    echo ""
    echo "Usage: ./setup-all.sh /path/to/AssemblyLine/shared/python"
    echo ""
    echo "The shared library is at: AssemblyLine/shared/python/"
    exit 1
fi

# Bundle into each service
for service in chat-service rag-service; do
    echo ""
    echo "[$service] Bundling assemblyline-common..."
    cd "$SCRIPT_DIR/$service"
    ./setup.sh "$SHARED_DIR"
done

echo ""
echo "============================================="
echo "Setup complete! Next steps:"
echo "  docker compose up -d          # Start all services"
echo "  cd electron-app && npm start  # Start desktop app"
echo "============================================="
