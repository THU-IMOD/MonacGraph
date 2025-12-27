#!/bin/bash

# Clean up script for LSM project
# This script removes workspace directories, VSCode counter data, and build artifacts

echo "Starting cleanup..."

# Remove .VSCodeCounter directory
rm -rf ./.VSCodeCounter

# Remove workspace directory in current directory
echo "Removing ./workspace..."
rm -rf ./workspace

# Remove workspace directories in all subdirectories
echo "Removing */workspace..."
rm -rf */workspace

# Clean cargo build artifacts
echo "Running cargo clean..."
cargo clean

echo "Cleanup completed!"