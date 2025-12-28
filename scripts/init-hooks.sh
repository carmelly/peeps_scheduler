#!/bin/bash
# Initialize git hooks for the project
# Run this after cloning or creating a new worktree

set -e

echo "Initializing git hooks..."
git config core.hooksPath .githooks

echo "✓ Git hooks configured"
echo ""
echo "Hooks are now active. Pre-commit validation will run on:"
echo "  - Markdown files (all repositories)"
