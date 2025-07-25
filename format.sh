#!/bin/bash

# kv-db Code Formatting Script
# This script applies various formatters to ensure consistent code style

set -e

echo "🔧 Running Go formatters..."

# 1. gofmt - basic Go formatting
echo "📝 Running gofmt..."
gofmt -w .

# 2. goimports - organize imports
echo "📦 Running goimports..."
goimports -w -local github.com/EzhovAndrew/kv-db .

# 3. gofumpt - stricter formatting
echo "✨ Running gofumpt (stricter formatting)..."
gofumpt -w .

# 4. go mod tidy - clean up go.mod
echo "🧹 Cleaning up go.mod..."
go mod tidy

echo "✅ Formatting complete!"
echo ""
echo "📊 Summary of changes (if any):"
git diff --stat || echo "No changes detected" 