#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEPH_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$CEPH_ROOT/build}"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Run CLI tests for Ceph tools using the cram testing framework.

Options:
    -h, --help      Show this help message
    -b, --build     Specify build directory (default: $CEPH_ROOT/build)

Examples:
    # Run all CLI tests:
    $0

    # Run with custom build directory:
    $0 -b /path/to/build

EOF
    exit 0
}

# Parse options
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -b|--build)
            BUILD_DIR="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Verify build directory exists
if [ ! -d "$BUILD_DIR" ]; then
    echo "Error: Build directory not found: $BUILD_DIR"
    echo "Please build Ceph first or specify correct build directory with -b"
    exit 1
fi

# Verify build/bin directory exists
if [ ! -d "$BUILD_DIR/bin" ]; then
    echo "Error: Build binaries not found in $BUILD_DIR/bin"
    echo "Please run './do_cmake.sh && cd build && ninja' first"
    exit 1
fi

# Change to build directory and run tests
cd "$BUILD_DIR"
export PATH="$PWD/bin:$PATH"

echo "Running all CLI tests from build directory: $BUILD_DIR"
echo "=================================================="
exec "$CEPH_ROOT/src/test/run-cli-tests"
