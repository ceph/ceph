#!/bin/bash
#
# Start a filesystem-backed RGW vstart cluster for development testing.
#
# Wraps vstart.sh to handle user bootstrap, cleanup, and optional GPFS
# integration.  See README.md in this directory for background.
#
# Usage:
#   src/script/rgw/rgw-vstart.sh [OPTIONS]
#
# Options:
#   --store nsfs|posix  Backend store (default: nsfs)
#   --gpfs              Redirect data root to a GPFS mount (nsfs only)
#   --clone             Enable GPFS clone_snap+clone_copy (implies --gpfs)
#   --gpfs-root DIR     GPFS data directory (default: /mnt/rgw/nsfs)
#   --debug-rgw N       Debug level (default: 20)
#
# Must be run from the build directory.

set -euo pipefail

STORE=nsfs
GPFS=false
CLONE=false
GPFS_ROOT=/mnt/rgw/nsfs
DEBUG_RGW=20

while [[ $# -gt 0 ]]; do
	case "$1" in
		--store)      STORE="$2"; shift 2 ;;
		--gpfs)       GPFS=true; shift ;;
		--clone)      CLONE=true; GPFS=true; shift ;;
		--gpfs-root)  GPFS_ROOT="$2"; shift 2 ;;
		--debug-rgw)  DEBUG_RGW="$2"; shift 2 ;;
		*) echo "unknown option: $1" >&2; exit 1 ;;
	esac
done

if [[ "$STORE" != "nsfs" && "$STORE" != "posix" ]]; then
	echo "error: --store must be nsfs or posix" >&2
	exit 1
fi

if $GPFS && [[ "$STORE" != "nsfs" ]]; then
	echo "error: --gpfs only applies to --store nsfs" >&2
	exit 1
fi

BUILD_DIR="$(pwd)"
if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
	echo "error: run this from the build directory" >&2
	exit 1
fi

SRC_DIR="$(grep -m1 'CMAKE_HOME_DIRECTORY' CMakeCache.txt | cut -d= -f2)"
CONF="$BUILD_DIR/ceph.conf"
PID="$BUILD_DIR/out/radosgw.8000.pid"

# --- kill any existing radosgw ---

echo "==> killing any existing radosgw on port 8000"
if [[ -f "$PID" ]]; then
	kill "$(cat "$PID")" 2>/dev/null || true
	sleep 1
fi
fuser -k 8000/tcp 2>/dev/null || true

# --- clean data dirs ---

echo "==> cleaning data dirs"
rm -rf "$BUILD_DIR/dev/rgw/$STORE"/{lmdb,root,userdb}/*

if $GPFS; then
	mkdir -p "$GPFS_ROOT"/{root,lmdb,userdb}

	# GPFS clone parents are immutable snapshots created by
	# gpfs_clone_snap().  The kernel prevents rm/unlink on them
	# with EROFS ("Read-only file system").
	#
	# To remove them:
	#   1. mmclone split <file>  — copies shared data blocks into
	#      the child, breaking the parent-child relationship and
	#      making both files independent regular files.
	#   2. rm <file>             — now works normally.
	#
	# mmclone is the IBM Storage Scale (GPFS) CLI for clone
	# management.  It lives at /usr/lpp/mmfs/bin/mmclone and wraps
	# the same libgpfs APIs (gpfs_clone_snap, gpfs_clone_copy,
	# gpfs_clone_unsnap) that the nsfs driver uses at runtime.
	#
	# The .clone_parent.* naming convention is set by the nsfs
	# driver's clone_parent_name() in fs_strategy.cc.
	MMCLONE=/usr/lpp/mmfs/bin/mmclone
	if [[ -x "$MMCLONE" ]]; then
		find "$GPFS_ROOT/root" -name '.clone_parent.*' -print0 2>/dev/null \
			| xargs -0 -r "$MMCLONE" split 2>/dev/null || true
	fi

	rm -rf "$GPFS_ROOT"/{root,lmdb,userdb}/*
fi

# --- run vstart.sh (bootstraps users + config) ---

echo "==> running vstart.sh --rgw_store $STORE"
MON=0 OSD=0 MDS=0 MGR=0 RGW=1 \
	"$SRC_DIR/src/vstart.sh" -n -d \
	--rgw_store "$STORE" \
	-o "rgw_${STORE}_cache_max_buckets=500" \
	-o 'rgw_multipart_min_part_size=32'

# --- for non-GPFS, vstart already started the daemon ---

if ! $GPFS; then
	DATA_DIR="$BUILD_DIR/dev/rgw/$STORE/root"
	echo "==> radosgw up on port 8000 (data on $DATA_DIR)"
	exit 0
fi

# --- GPFS: kill vstart daemon, patch ceph.conf, restart ---
#
# vstart.sh bootstraps test users into the config DB and userdb under
# the build directory, then starts a daemon pointing at build-dir
# paths.  GPFS operations (gpfs_linkat, clone_snap, etc.) require
# the data root to be on a GPFS filesystem, so we:
#   1. kill the vstart-spawned daemon
#   2. patch ceph.conf to redirect the base path to the GPFS mount
#   3. restart the daemon ourselves
#
# The userdb and LMDB dirs stay in the build directory — they don't
# need to be on GPFS, and keeping them there avoids re-bootstrapping
# users.

echo "==> killing vstart-spawned daemon"
kill "$(cat "$PID")" 2>/dev/null || true
sleep 1

echo "==> patching ceph.conf for GPFS base path"
sed -i "s|rgw nsfs base path = .*|rgw nsfs base path = $GPFS_ROOT/root|" "$CONF"

if $CLONE; then
	if grep -q 'rgw nsfs gpfs clone files' "$CONF"; then
		sed -i 's/rgw nsfs gpfs clone files = .*/rgw nsfs gpfs clone files = true/' "$CONF"
	else
		sed -i "/rgw nsfs base path/a\\        rgw nsfs gpfs clone files = true" "$CONF"
	fi
	echo "    clone_files = true"
fi

echo "==> starting radosgw"
"$BUILD_DIR/bin/radosgw" \
	-c "$CONF" \
	--log-file="$BUILD_DIR/out/radosgw.8000.log" \
	--admin-socket="$BUILD_DIR/out/radosgw.8000.asok" \
	--pid-file="$PID" \
	--rgw_luarocks_location="$BUILD_DIR/out/radosgw.8000.luarocks" \
	--debug-rgw="$DEBUG_RGW" \
	--debug-ms=0 \
	-n client.rgw.8000 \
	--rgw_frontends='beast port=8000'

sleep 2
if fuser 8000/tcp >/dev/null 2>&1; then
	echo "==> radosgw up on port 8000 (data on $GPFS_ROOT/root)"
else
	echo "error: radosgw did not start" >&2
	tail -20 "$BUILD_DIR/out/radosgw.8000.log"
	exit 1
fi
