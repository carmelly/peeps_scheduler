#!/bin/bash
# Markdown validation and formatting script
# Uses prettier for formatting and markdownlint for linting
#
# Usage:
#   ./scripts/check-md.sh [--fix] [path]
#
# Examples:
#   ./scripts/check-md.sh                          # Check all markdown
#   ./scripts/check-md.sh README.md                # Check single file
#   ./scripts/check-md.sh --fix README.md          # Fix single file
#   ./scripts/check-md.sh --fix docs/              # Fix directory

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Paths - relative to script location (main repo root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PRETTIER="npx prettier"
MARKDOWNLINT="npx markdownlint-cli2"
PRETTIER_CONFIG="$SCRIPT_DIR/.prettierrc"
MARKDOWNLINT_CONFIG="$SCRIPT_DIR/.markdownlint-cli2.jsonc"

# Parse arguments
FIX=false
TARGET=""

for arg in "$@"; do
    case $arg in
        --fix)
            FIX=true
            shift
            ;;
        *)
            TARGET="$arg"
            ;;
    esac
done

# Require explicit target when using --fix
if [ "$FIX" = true ] && [ -z "$TARGET" ]; then
    echo -e "${RED}✗${NC} --fix requires an explicit file or directory target"
    echo ""
    echo "Usage:"
    echo "  ./scripts/check-md.sh --fix <file-or-directory>"
    echo ""
    echo "Examples:"
    echo "  ./scripts/check-md.sh --fix README.md"
    echo "  ./scripts/check-md.sh --fix docs/"
    echo "  ./scripts/check-md.sh --fix peeps-config/overlay/.claude/"
    exit 1
fi

# Default to current directory for check-only mode
if [ -z "$TARGET" ]; then
    TARGET="."
fi

# Check if node_modules exists (in main repo root)
if [ ! -d "$SCRIPT_DIR/node_modules" ]; then
    echo -e "${YELLOW}ℹ${NC} Installing dependencies..."
    (cd "$SCRIPT_DIR" && npm install)
fi

echo -e "${YELLOW}ℹ${NC} Running markdown validation on: $TARGET"
echo ""

# Run prettier
if [ "$FIX" = true ]; then
    echo -e "${YELLOW}→${NC} Running prettier (format)..."
    if [ -f "$TARGET" ]; then
        $PRETTIER --config "$PRETTIER_CONFIG" --write "$TARGET"
    else
        $PRETTIER --config "$PRETTIER_CONFIG" --write "$TARGET/**/*.md"
    fi
    echo -e "${GREEN}✓${NC} Prettier formatting complete"
else
    echo -e "${YELLOW}→${NC} Running prettier (check)..."
    if [ -f "$TARGET" ]; then
        $PRETTIER --config "$PRETTIER_CONFIG" --check "$TARGET"
    else
        $PRETTIER --config "$PRETTIER_CONFIG" --check "$TARGET/**/*.md"
    fi
    echo -e "${GREEN}✓${NC} Prettier check passed"
fi

echo ""

# Run markdownlint
# Note: When targeting a specific file, we use a config without globs to avoid
# processing all markdown files in the project
if [ "$FIX" = true ]; then
    echo -e "${YELLOW}→${NC} Running markdownlint (fix)..."
    if [ -f "$TARGET" ]; then
        # For single files, don't use globs from config - just process the file
        $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" --fix "$TARGET" || true
    else
        # For directories, use the config with globs
        $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" --fix "$TARGET/**/*.md" || true
    fi
    echo -e "${GREEN}✓${NC} Markdownlint auto-fixes applied"

    # Check for remaining issues
    echo ""
    echo -e "${YELLOW}→${NC} Checking for remaining issues..."
    if [ -f "$TARGET" ]; then
        if $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" "$TARGET" 2>&1 | grep -q "MD"; then
            echo -e "${YELLOW}⚠${NC} Some issues require manual fixes:"
            $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" "$TARGET" || true
            exit 1
        else
            echo -e "${GREEN}✓${NC} All issues fixed!"
        fi
    else
        if $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" "$TARGET/**/*.md" 2>&1 | grep -q "MD"; then
            echo -e "${YELLOW}⚠${NC} Some issues require manual fixes:"
            $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" "$TARGET/**/*.md" || true
            exit 1
        else
            echo -e "${GREEN}✓${NC} All issues fixed!"
        fi
    fi
else
    echo -e "${YELLOW}→${NC} Running markdownlint (check)..."
    if [ -f "$TARGET" ]; then
        $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" "$TARGET"
    else
        $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" "$TARGET/**/*.md"
    fi
    echo -e "${GREEN}✓${NC} Markdownlint check passed"
fi

echo ""
echo -e "${GREEN}✓${NC} Markdown validation complete!"