#!/bin/bash
# Markdown validation and formatting script
# Uses prettier for formatting and markdownlint for linting
#
# Usage:
#   ./scripts/check-md.sh [--fix|--errors-only] [path]
#
# Examples:
#   ./scripts/check-md.sh                          # Check all markdown
#   ./scripts/check-md.sh README.md                # Check single file
#   ./scripts/check-md.sh --fix README.md          # Fix single file
#   ./scripts/check-md.sh --fix docs/              # Fix directory
#   ./scripts/check-md.sh --errors-only README.md  # Show errors with line numbers only

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
ERRORS_ONLY=false
FILES=()

for arg in "$@"; do
    case $arg in
        --fix)
            FIX=true
            ;;
        --errors-only)
            ERRORS_ONLY=true
            ;;
        *)
            FILES+=("$arg")
            ;;
    esac
done

# Require explicit target when using --fix
if [ "$FIX" = true ] && [ ${#FILES[@]} -eq 0 ]; then
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
if [ ${#FILES[@]} -eq 0 ]; then
    FILES=(".")
fi

# Check if node_modules exists (in main repo root)
if [ ! -d "$SCRIPT_DIR/node_modules" ]; then
    echo -e "${YELLOW}ℹ${NC} Installing dependencies..."
    (cd "$SCRIPT_DIR" && npm install)
fi

# Handle --errors-only mode (skip formatting, just show violations)
if [ "$ERRORS_ONLY" = true ]; then
    for file in "${FILES[@]}"; do
        if [ -f "$file" ]; then
            $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" "$file" 2>&1 | grep "MD" | sed 's/ \[Context:.*\]$//' || true
        else
            $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" "$file/**/*.md" 2>&1 | grep "MD" | sed 's/ \[Context:.*\]$//' || true
        fi
    done
    exit 0
fi

echo -e "${YELLOW}ℹ${NC} Running markdown validation on: ${FILES[*]}"
echo ""

# Separate files from directories
REGULAR_FILES=()
DIRECTORIES=()
for item in "${FILES[@]}"; do
    if [ -f "$item" ]; then
        REGULAR_FILES+=("$item")
    elif [ -d "$item" ]; then
        DIRECTORIES+=("$item")
    fi
done

# Run prettier
if [ "$FIX" = true ]; then
    echo -e "${YELLOW}→${NC} Running prettier (format)..."
    # Process all regular files at once
    if [ ${#REGULAR_FILES[@]} -gt 0 ]; then
        $PRETTIER --config "$PRETTIER_CONFIG" --write "${REGULAR_FILES[@]}"
    fi
    # Process directories
    for dir in "${DIRECTORIES[@]}"; do
        $PRETTIER --config "$PRETTIER_CONFIG" --write "$dir/**/*.md"
    done
    echo -e "${GREEN}✓${NC} Prettier formatting complete"
else
    echo -e "${YELLOW}→${NC} Running prettier (check)..."
    # Process all regular files at once
    if [ ${#REGULAR_FILES[@]} -gt 0 ]; then
        $PRETTIER --config "$PRETTIER_CONFIG" --check "${REGULAR_FILES[@]}"
    fi
    # Process directories
    for dir in "${DIRECTORIES[@]}"; do
        $PRETTIER --config "$PRETTIER_CONFIG" --check "$dir/**/*.md"
    done
    echo -e "${GREEN}✓${NC} Prettier check passed"
fi

echo ""

# Run markdownlint
if [ "$FIX" = true ]; then
    echo -e "${YELLOW}→${NC} Running markdownlint (fix)..."
    # Process all regular files at once
    if [ ${#REGULAR_FILES[@]} -gt 0 ]; then
        $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" --fix "${REGULAR_FILES[@]}" || true
    fi
    # Process directories
    for dir in "${DIRECTORIES[@]}"; do
        $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" --fix "$dir/**/*.md" || true
    done
    echo -e "${GREEN}✓${NC} Markdownlint auto-fixes applied"

    # Check for remaining issues
    echo ""
    echo -e "${YELLOW}→${NC} Checking for remaining issues..."
    HAS_ISSUES=false
    # Check all regular files at once
    if [ ${#REGULAR_FILES[@]} -gt 0 ]; then
        if $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" "${REGULAR_FILES[@]}" 2>&1 | grep -q "MD"; then
            HAS_ISSUES=true
        fi
    fi
    # Check directories
    for dir in "${DIRECTORIES[@]}"; do
        if $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" "$dir/**/*.md" 2>&1 | grep -q "MD"; then
            HAS_ISSUES=true
        fi
    done

    if [ "$HAS_ISSUES" = true ]; then
        echo -e "${YELLOW}⚠${NC} Some issues require manual fixes:"
        # Show errors for regular files
        if [ ${#REGULAR_FILES[@]} -gt 0 ]; then
            $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" "${REGULAR_FILES[@]}" || true
        fi
        # Show errors for directories
        for dir in "${DIRECTORIES[@]}"; do
            $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" "$dir/**/*.md" || true
        done
        exit 1
    else
        echo -e "${GREEN}✓${NC} All issues fixed!"
    fi
else
    echo -e "${YELLOW}→${NC} Running markdownlint (check)..."
    # Check all regular files at once
    if [ ${#REGULAR_FILES[@]} -gt 0 ]; then
        $MARKDOWNLINT --no-globs --config "$MARKDOWNLINT_CONFIG" "${REGULAR_FILES[@]}"
    fi
    # Check directories
    for dir in "${DIRECTORIES[@]}"; do
        $MARKDOWNLINT --config "$MARKDOWNLINT_CONFIG" "$dir/**/*.md"
    done
    echo -e "${GREEN}✓${NC} Markdownlint check passed"
fi

echo ""
echo -e "${GREEN}✓${NC} Markdown validation complete!"