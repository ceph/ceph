#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/bin/bash
set -euo pipefail
# Activate gcc-toolset-12 (required for C++23 <format> support)
GCC_TOOLSET_ENABLE="/opt/rh/gcc-toolset-12/enable"
if [[ -f "$GCC_TOOLSET_ENABLE" ]]; then
    source "$GCC_TOOLSET_ENABLE"
else
    echo "ERROR: gcc-toolset-12 not found at $GCC_TOOLSET_ENABLE"
    echo "Install it with:  sudo dnf install gcc-toolset-12"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Building kv_poc from source ==="
echo "Root: $ROOT"

# Prerequisites check
for cmd in cmake make go protoc; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: $cmd not found in PATH"
        exit 1
    fi
done

# Install Go protoc plugins if missing
GOBIN="$(go env GOPATH)/bin"
export PATH="$PATH:$GOBIN"

if ! command -v protoc-gen-go &>/dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

# Build C++ backend
echo ""
echo "=== Building C++ backend ==="
cmake -B "$ROOT/build" -S "$ROOT/backend"
cmake --build "$ROOT/build" -j"$(nproc)"
echo "Backend: $ROOT/build/kv-rgw-backend"

# Generate proto + build Go frontend
echo ""
echo "=== Building Go frontend ==="
cd "$ROOT/frontend"
mkdir -p pb
protoc --go_out=pb --go_opt=paths=source_relative \
    -I ../proto ../proto/kvrgw.proto
go build -o ../build/kv-rgw-frontend .
echo "Frontend: $ROOT/build/kv-rgw-frontend"

echo ""
echo "=== Build complete ==="
echo ""
echo "To start the system:"
echo "  cd $ROOT"
echo "  pkill -f kv-rgw-frontend; pkill -f kv-rgw-backend; pkill nginx; sleep 3"
echo "  bash scripts/reload.sh --clean 1"
