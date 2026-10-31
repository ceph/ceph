#!/usr/bin/env bash

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${ROOT}/third_party/fdb"
VER="${FDB_VERSION:-7.3.69}"   # any 7.3.x matching the RPMs you download
mkdir -p "$DEST"
# download foundationdb-clients-${VER}*.x86_64.rpm and foundationdb-server-...
cd "$DEST"
rpm2cpio /path/to/foundationdb-clients-*.rpm | cpio -idmv
rpm2cpio /path/to/foundationdb-server-*.rpm | cpio -idmv
test -f usr/lib64/libfdb_c.so
test -x usr/sbin/fdbserver
