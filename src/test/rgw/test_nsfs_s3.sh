#!/bin/bash
#
# test_nsfs_s3.sh — integration test for nsfs hierarchical path mapping
#
# Runs against a vstart.sh cluster with --rgw_store nsfs.
# Self-contained: starts/stops the cluster, uses inline credentials.
#
# Usage:
#   cd <build-dir>
#   ../src/test/rgw/test_nsfs_s3.sh [--no-start] [--no-stop]
#
# Options:
#   --no-start   assume cluster is already running (skip vstart)
#   --no-stop    leave cluster running after tests (for debugging)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$(pwd)"

# well-known vstart test credentials
ACCESS_KEY="0555b35654ad1656d804"
SECRET_KEY="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
HOST="localhost"
PORT="8000"

DO_START=1
DO_STOP=1
VERBOSE=0

for arg in "$@"; do
  case "$arg" in
    --no-start) DO_START=0 ;;
    --no-stop)  DO_STOP=0 ;;
    --verbose)  VERBOSE=1 ;;
  esac
done

NSFS_ROOT="$BUILD_DIR/dev/rgw/nsfs/root"
S3CFG=$(mktemp /tmp/nsfs-test-s3cfg.XXXXXX)
PASS=0
FAIL=0
TESTS=0

stop_rgw() {
  local pidfile="$BUILD_DIR/out/radosgw.${PORT}.pid"
  if [ -f "$pidfile" ]; then
    kill $(cat "$pidfile") 2>/dev/null || true
    rm -f "$pidfile"
    sleep 1
  fi
}

cleanup() {
  rm -f "$S3CFG"
  if [ "$DO_STOP" -eq 1 ] && [ "$DO_START" -eq 1 ]; then
    stop_rgw
  fi
}
trap cleanup EXIT

cat > "$S3CFG" <<EOF
[default]
access_key = $ACCESS_KEY
secret_key = $SECRET_KEY
host_base = $HOST:$PORT
host_bucket = $HOST:$PORT/%(bucket)
use_https = False
signature_v2 = True
EOF

s3() {
  [ "$VERBOSE" -eq 1 ] && echo "  + s3cmd $*" >&2
  s3cmd -c "$S3CFG" "$@"
}

log() {
  echo "=== $*"
}

check() {
  local desc="$1"
  shift
  TESTS=$((TESTS + 1))
  [ "$VERBOSE" -eq 1 ] && echo "  + check: $*" >&2
  if eval "$@"; then
    PASS=$((PASS + 1))
    [ "$VERBOSE" -eq 1 ] && echo "  PASS: $desc"
  else
    FAIL=$((FAIL + 1))
    echo "  FAIL: $desc"
  fi
}

# --- cluster start ---

if [ "$DO_START" -eq 1 ]; then
  log "stopping any existing radosgw"
  stop_rgw

  log "starting vstart cluster with nsfs backend"
  cd "$BUILD_DIR"
  if [ "$VERBOSE" -eq 1 ]; then
    MON=0 OSD=0 MDS=0 MGR=0 RGW=1 \
      "$SRC_DIR/vstart.sh" -n -d --rgw_store nsfs \
      -o rgw_nsfs_cache_max_buckets=500
  else
    MON=0 OSD=0 MDS=0 MGR=0 RGW=1 \
      "$SRC_DIR/vstart.sh" -n -d --rgw_store nsfs \
      -o rgw_nsfs_cache_max_buckets=500 \
      > /dev/null 2>&1
  fi
  sleep 2
fi

# --- test bucket creation ---

BUCKET="nsfs-test-$$"
log "create bucket $BUCKET"
s3 mb "s3://$BUCKET" > /dev/null
check "bucket dir exists" '[ -d "$NSFS_ROOT/$BUCKET" ]'

# --- test flat object PUT/GET ---

log "flat object PUT/GET"
echo "flat content" > /tmp/nsfs-flat-$$.txt
s3 put /tmp/nsfs-flat-$$.txt "s3://$BUCKET/flat.txt" > /dev/null
check "flat file on disk" '[ -f "$NSFS_ROOT/$BUCKET/flat.txt" ]'

s3 get "s3://$BUCKET/flat.txt" /tmp/nsfs-flat-get-$$.txt > /dev/null
check "flat GET content matches" 'diff -q /tmp/nsfs-flat-$$.txt /tmp/nsfs-flat-get-$$.txt > /dev/null'

# --- test hierarchical PUT ---

log "hierarchical PUT"
echo "nested content" > /tmp/nsfs-nested-$$.txt
s3 put /tmp/nsfs-nested-$$.txt "s3://$BUCKET/dir1/dir2/file.txt" > /dev/null 2>&1
check "dir1/ exists" '[ -d "$NSFS_ROOT/$BUCKET/dir1" ]'
check "dir1/dir2/ exists" '[ -d "$NSFS_ROOT/$BUCKET/dir1/dir2" ]'
check "dir1/dir2/file.txt on disk" '[ -f "$NSFS_ROOT/$BUCKET/dir1/dir2/file.txt" ]'

# --- test hierarchical GET ---

log "hierarchical GET"
s3 get "s3://$BUCKET/dir1/dir2/file.txt" /tmp/nsfs-nested-get-$$.txt > /dev/null 2>&1
check "hierarchical GET content matches" 'diff -q /tmp/nsfs-nested-$$.txt /tmp/nsfs-nested-get-$$.txt > /dev/null'

# --- test multiple objects in same directory ---

log "sibling objects"
echo "sibling1" > /tmp/nsfs-sib1-$$.txt
echo "sibling2" > /tmp/nsfs-sib2-$$.txt
s3 put /tmp/nsfs-sib1-$$.txt "s3://$BUCKET/shared/a.txt" > /dev/null 2>&1
s3 put /tmp/nsfs-sib2-$$.txt "s3://$BUCKET/shared/b.txt" > /dev/null 2>&1
check "shared/a.txt on disk" '[ -f "$NSFS_ROOT/$BUCKET/shared/a.txt" ]'
check "shared/b.txt on disk" '[ -f "$NSFS_ROOT/$BUCKET/shared/b.txt" ]'

s3 get "s3://$BUCKET/shared/a.txt" /tmp/nsfs-sib1-get-$$.txt > /dev/null 2>&1
s3 get "s3://$BUCKET/shared/b.txt" /tmp/nsfs-sib2-get-$$.txt > /dev/null 2>&1
check "sibling a GET matches" 'diff -q /tmp/nsfs-sib1-$$.txt /tmp/nsfs-sib1-get-$$.txt > /dev/null'
check "sibling b GET matches" 'diff -q /tmp/nsfs-sib2-$$.txt /tmp/nsfs-sib2-get-$$.txt > /dev/null'

# --- test listing ---

log "listing"
LIST_OUT=$(s3 ls "s3://$BUCKET/" 2>/dev/null || true)
check "listing returns something" '[ -n "$LIST_OUT" ]'
check "listing shows flat.txt" 'echo "$LIST_OUT" | grep -q "flat.txt"'
check "listing shows dir1/" 'echo "$LIST_OUT" | grep -q "dir1/"'

LIST_DIR1=$(s3 ls "s3://$BUCKET/dir1/" 2>/dev/null || true)
check "dir1/ listing shows dir2/" 'echo "$LIST_DIR1" | grep -q "dir2/"'

LIST_DIR2=$(s3 ls "s3://$BUCKET/dir1/dir2/" 2>/dev/null || true)
check "dir1/dir2/ listing shows file.txt" 'echo "$LIST_DIR2" | grep -q "file.txt"'

LIST_SHARED=$(s3 ls "s3://$BUCKET/shared/" 2>/dev/null || true)
check "shared/ listing shows a.txt" 'echo "$LIST_SHARED" | grep -q "a.txt"'
check "shared/ listing shows b.txt" 'echo "$LIST_SHARED" | grep -q "b.txt"'

if [ "$VERBOSE" -eq 1 ]; then
  echo "  bucket listing:"
  echo "$LIST_OUT" | sed 's/^/    /'
  echo "  dir1/ listing:"
  echo "$LIST_DIR1" | sed 's/^/    /'
  echo "  shared/ listing:"
  echo "$LIST_SHARED" | sed 's/^/    /'
fi

# --- test DELETE with directory cleanup ---

log "hierarchical DELETE"
s3 del "s3://$BUCKET/dir1/dir2/file.txt" > /dev/null 2>&1
check "file.txt removed from disk" '[ ! -f "$NSFS_ROOT/$BUCKET/dir1/dir2/file.txt" ]'
check "dir2/ cleaned up" '[ ! -d "$NSFS_ROOT/$BUCKET/dir1/dir2" ]'
check "dir1/ cleaned up" '[ ! -d "$NSFS_ROOT/$BUCKET/dir1" ]'

log "DELETE preserves neighbors"
check "shared/ still exists" '[ -d "$NSFS_ROOT/$BUCKET/shared" ]'
s3 del "s3://$BUCKET/shared/a.txt" > /dev/null 2>&1
check "a.txt removed" '[ ! -f "$NSFS_ROOT/$BUCKET/shared/a.txt" ]'
check "shared/ preserved (b.txt remains)" '[ -d "$NSFS_ROOT/$BUCKET/shared" ]'
check "b.txt still exists" '[ -f "$NSFS_ROOT/$BUCKET/shared/b.txt" ]'

# --- filesystem layout dump ---

if [ "$VERBOSE" -eq 1 ]; then
  log "filesystem layout"
  find "$NSFS_ROOT/$BUCKET" | sed "s|$NSFS_ROOT/||" | sort | sed 's/^/    /'
fi

# --- cleanup temp files ---

rm -f /tmp/nsfs-flat-$$.txt /tmp/nsfs-flat-get-$$.txt
rm -f /tmp/nsfs-nested-$$.txt /tmp/nsfs-nested-get-$$.txt
rm -f /tmp/nsfs-sib1-$$.txt /tmp/nsfs-sib1-get-$$.txt
rm -f /tmp/nsfs-sib2-$$.txt /tmp/nsfs-sib2-get-$$.txt

# --- summary ---

echo ""
echo "=== $TESTS tests: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
