#!/bin/bash

# Script to run all unit tests in the kv-db repository

set -u

echo "🔍 Discovering and running all unit tests in kv-db repository..."
echo "=================================================="

# Initialize counters
total_packages=0
passed_packages=0
failed_packages=0

# Find all directories containing *_test.go files
test_dirs=$(find . -name "*_test.go" -type f | xargs -I {} dirname {} | sort -u)

if [ -z "$test_dirs" ]; then
    echo "❌ No test files found in the repository"
    exit 1
fi

echo "📦 Found test packages:"
while IFS= read -r dir; do
    echo "  - $dir"
    ((total_packages++))
done <<< "$test_dirs"
echo ""

echo "🚀 Running tests..."
while IFS= read -r dir; do
    echo "----------------------------------------"
    
    if cd "$dir" && gotestsum --format dots .; then
        echo "✅ PASSED: $dir"
        ((passed_packages++))
    else
        echo "❌ FAILED: $dir"
        ((failed_packages++))
    fi
    
    # Return to repository root
    cd - > /dev/null
    echo ""
done <<< "$test_dirs"


echo "=================================================="
echo "📊 Test Summary:"
echo "  Total packages: $total_packages"
echo "  Passed: $passed_packages"
echo "  Failed: $failed_packages"
echo ""

if [ $failed_packages -eq 0 ]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "💥 Some tests failed!"
    exit 1
fi
