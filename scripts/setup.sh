#!/bin/bash
# Setup script for peeps-scheduler
# Initializes git hooks and project configuration
# Usage: ./scripts/setup.sh

set -e

# ANSI colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "success") echo -e "${GREEN}✓${NC} $message" ;;
        "error") echo -e "${RED}✗${NC} $message" ;;
        "info") echo -e "${YELLOW}ℹ${NC} $message" ;;
        *) echo "$message" ;;
    esac
}

print_status "info" "Setting up project..."
echo ""

# Initialize hooks for main repo
print_status "info" "Initializing git hooks..."
git config core.hooksPath .githooks
print_status "success" "Git hooks configured"

echo ""

# Hydrate symlinks and configure submodule hooks (if peeps-config exists)
if [ -d "peeps-config" ]; then
    print_status "info" "Configuring submodules..."
    ./peeps-config/scripts/hydrate.sh
else
    print_status "info" "peeps-config not found, skipping submodule setup"
fi

echo ""
print_status "success" "Setup complete!"
