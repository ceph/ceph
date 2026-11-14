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
#   --lwe               Enable GPFS LWE cluster-wide locking (implies --gpfs)
#   --gpfs-root DIR     GPFS data directory (default: /mnt/rgw/nsfs)
#   --clean             Wipe config DB and all data before starting
#   --debug-rgw N       Debug level (default: 20)
#   --with-keycloak     Start a local Keycloak container for WebIdentity
#                       testing (requires podman, jq, openssl, curl)
#   --with-kafka        Start a local Kafka broker for notification testing
#                       (requires podman)
#   --with-vault        Start a local HashiCorp Vault for SSE-KMS / SSE-S3
#                       testing (requires podman, curl)
#   --with-lifecycle     Set rgw_lc_debug_interval=10 so lifecycle
#                       expiration tests complete in seconds, and
#                       uncomment lc_debug_interval in s3tests.conf
#   --with-inotify      Enable inotify watcher on bucket directories
#                       (for sideloaded file detection; off by default)
#   --ramdisk           Mount tmpfs on data root and LMDB cache dirs
#                       (eliminates I/O latency for test runs)
#   --bucket-profile P  shared | strong:  the profile new nsfs buckets are
#                       marked with.  The driver defaults to shared;  FSIO
#                       (librgw, the librgw_file tests) needs strong.
#   --data-root DIR     Serve objects from DIR/root instead of the build
#                       directory (e.g. --data-root /mnt/nvme for fast
#                       storage, or a tree another gateway wrote).
#                       The LMDB cache and userdb stay in the build dir.
#                       Rejects --clean:  DIR/root is not ours to wipe.
#   --perf              Performance tuning: NUMA pinning, tcmalloc
#                       thread cache, tcp_nodelay, no request timeout
#   -o 'key=value'      Pass extra config option through to vstart.sh
#                       (e.g. -o 'rgw_posix_sync_policy=relaxed')
#   --profile           Start without debug logging (no -d to vstart.sh)
#                       for performance profiling; sets all debug to 0
#   --foreground        Print the radosgw command instead of running it;
#                       use this to start the daemon as root in another
#                       terminal (LWE requires root for DMAPI handles)
#   --restart           Stop the running radosgw and start it again on
#                       the same ceph.conf:  no vstart.sh, nothing cleaned,
#                       so the userdb, the LMDB cache and the tree are as
#                       they were.  Takes only --store, --adoption-mode
#                       (patched into ceph.conf), --debug-rgw, --port, --perf and
#                       --with-impersonation, which is on the command line
#                       rather than in ceph.conf and so is given again.
#
# Must be run from the build directory.

set -euo pipefail

STORE=nsfs
GPFS=false
CLONE=false
LWE=false
CLEAN=false
KEYCLOAK=false
KAFKA=false
VAULT=false
LIFECYCLE=false
INOTIFY=false
RAMDISK=false
DATA_ROOT=""
BUCKET_PROFILE=""
PERF=false
PROFILE=false
GPFS_ROOT=/mnt/rgw/nsfs
DEBUG_RGW=20
FOREGROUND=false
RESTART=false
EXTRA_OPTS=()

while [[ $# -gt 0 ]]; do
	case "$1" in
		--store)      STORE="$2"; shift 2 ;;
		--gpfs)       GPFS=true; shift ;;
		--clone)      CLONE=true; GPFS=true; shift ;;
		--lwe)        LWE=true; GPFS=true; shift ;;
		--clean)      CLEAN=true; shift ;;
		--with-keycloak) KEYCLOAK=true; shift ;;
		--with-kafka) KAFKA=true; shift ;;
		--with-vault) VAULT=true; shift ;;
		--with-lifecycle) LIFECYCLE=true; shift ;;
		--with-inotify) INOTIFY=true; shift ;;
		--ramdisk) RAMDISK=true; shift ;;
		--data-root) DATA_ROOT="$2"; shift 2 ;;
		--bucket-profile) BUCKET_PROFILE="$2"; shift 2 ;;
		--perf) PERF=true; shift ;;
		--profile) PROFILE=true; shift ;;
		--gpfs-root)  GPFS_ROOT="$2"; shift 2 ;;
		--debug-rgw)  DEBUG_RGW="$2"; shift 2 ;;
		--foreground) FOREGROUND=true; shift ;;
		--restart) RESTART=true; shift ;;
		-o) EXTRA_OPTS+=(-o "$2"); shift 2 ;;
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

# ceph.conf is regenerated by vstart.sh, so it is patched after it ran.
patch_conf_bool() {
	local key="$1" val="$2"
	if grep -q "$key" "$CONF"; then
		sed -i "s/$key = .*/$key = $val/" "$CONF"
	else
		sed -i "/rgw nsfs base path/a\\        $key = $val" "$CONF"
	fi
	echo "    $key = $val"
}

# --- build the radosgw command line, and start it ---

start_radosgw() {
	RGW_PREFIX=()

	if $PERF; then
		RGW_PREFIX=(
			numactl -N 0 -m 0 --
			env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=134217728
		)
		echo "==> perf: NUMA node 0, tcmalloc 128M thread cache, tcp_nodelay"
	fi

	RGW_CMD=(
		${RGW_PREFIX[@]+"${RGW_PREFIX[@]}"}
		"$BUILD_DIR/bin/radosgw"
		-c "$CONF"
		--pid-file="$PID"
		--debug-rgw="$DEBUG_RGW"
		--debug-ms=0
		--rgw_frontends="$BEAST_OPTS"
	)

	# On the gateway's command line, not in ceph.conf:  radosgw-admin reads
	# ceph.conf, and nsfs impersonates wherever it is configured to.
	if $IMPERSONATE; then
		RGW_CMD+=(--rgw_nsfs_impersonate=true)
	fi

	if $FOREGROUND; then
		# Print the command for the user to run (e.g. as root).
		# LWE requires root for DMAPI handle operations.
		echo ""
		echo "==> run the following command (e.g. as root for LWE):"
		echo ""
		local_cmd=""
		for arg in "${RGW_CMD[@]}"; do
			if [[ "$arg" == *" "* || "$arg" == *"'"* ]]; then
				local_cmd+=" '${arg}'"
			else
				local_cmd+=" ${arg}"
			fi
		done
		echo " ${local_cmd# }"
		echo ""
		if $IMPERSONATE; then
			echo "    (with --with-impersonation, run it as yourself, not" \
				"as root)"
		fi
		echo "data root: $GPFS_ROOT/root"
		exit 0
	fi

	echo "==> starting radosgw"
	"${RGW_CMD[@]}"

	sleep 2
	# ss, not fuser:  a radosgw given file capabilities is not dumpable, so
	# its /proc/<pid>/fd is root's and fuser cannot see the socket.
		exit 1
	fi
}


# --- --restart:  the running gateway again, on the same state ---

if $RESTART; then
	if $GPFS || $CLEAN || $KEYCLOAK || $KAFKA || $VAULT || $LIFECYCLE || \
		$NON_MD5_ETAG || $NO_FIXTURE || $INOTIFY || $RAMDISK || $PROFILE || \
		[[ -n "$NOOBAA_CONFIG_ROOT" ]] || [[ -n "$BUCKET_PROFILE" ]] || \
		[[ -n "$ACCESS_MODEL" ]] || (( ${#EXTRA_OPTS[@]} )); then
		echo "error: --restart takes only --store, --adoption-mode," \
			"in ceph.conf from the start it restarts" >&2
		exit 1
	fi
	if [[ ! -f "$CONF" ]]; then
		echo "error: no $CONF to restart on;  start without --restart" >&2
		exit 1
	fi
	echo "==> stopping radosgw"
	OLD_PID="$(cat "$PID" 2>/dev/null || true)"
	if [[ -n "$OLD_PID" ]]; then
		kill "$OLD_PID" 2>/dev/null || true
		for i in $(seq 1 30); do
			kill -0 "$OLD_PID" 2>/dev/null || break
			sleep 1
		done
		if kill -0 "$OLD_PID" 2>/dev/null; then
			echo "error: radosgw (pid $OLD_PID) did not stop" >&2
			exit 1
		fi
	fi
	if [[ -n "$ADOPTION_MODE" ]]; then
		case "$ADOPTION_MODE" in
			command|automatic) ;;
			*) echo "error: --adoption-mode must be command or automatic" >&2
			   exit 1 ;;
		esac
		patch_conf_bool "rgw nsfs adoption mode" "$ADOPTION_MODE"
	fi
	start_radosgw
	exit 0
fi

# --- kill any existing radosgw ---

echo "==> killing any existing radosgw on port 8000"
if [[ -f "$PID" ]]; then
	kill "$(cat "$PID" 2>/dev/null)" 2>/dev/null || true
	sleep 1
fi
fuser -k 8000/tcp 2>/dev/null || true

# stop any existing sidecar containers from a prior run
if command -v podman &>/dev/null; then
	podman stop keycloak-vstart 2>/dev/null || true
	podman rm keycloak-vstart 2>/dev/null || true
	podman stop kafka-vstart 2>/dev/null || true
	podman rm kafka-vstart 2>/dev/null || true
	podman stop vault-vstart 2>/dev/null || true
	podman rm vault-vstart 2>/dev/null || true
fi

# --- clean data dirs ---
# tolerate permission errors from root-owned files (e.g. LMDB
# created when the daemon ran as root for LWE testing)

# --- validate --data-root before anything is deleted -------------
#
# Early, because whether a redirect is in play decides what this
# script may remove.

if [[ -n "$DATA_ROOT" ]]; then
	if [[ ! -d "$DATA_ROOT" ]]; then
		echo "error: --data-root $DATA_ROOT does not exist" >&2
		exit 1
	fi
	if $CLEAN; then
		echo "error: --clean with --data-root would destroy the contents of" >&2
		echo "       $DATA_ROOT/root, which this script did not create and" >&2
		echo "       has no business removing.  Remove it yourself if that" >&2
		echo "       is what you want, then run again without --clean." >&2
		exit 1
	fi
fi

if $CLEAN; then
	echo "==> --clean: wiping config DB and all data"
	rm -f "$BUILD_DIR/dev/rgw/dbstore/config.db" 2>/dev/null || true
fi

echo "==> cleaning data dirs"
#
# Per entry, because `rm -rf dir/*` globs *through* a symlink and
# deletes what it points at.  --ramdisk leaves these as symlinks, so a
# second run used to empty the redirect target rather than the link.
for sub in lmdb root userdb; do
	d="$BUILD_DIR/dev/rgw/$STORE/$sub"
	if [[ -L "$d" ]]; then
		rm -f "$d" 2>/dev/null || true
	else
		rm -rf "$d"/* 2>/dev/null || true
	fi
done

if $RAMDISK; then
	RAMDISK_BASE="/dev/shm/rgw-ramdisk"
	if ! mountpoint -q "$RAMDISK_BASE" 2>/dev/null; then
		echo "error: $RAMDISK_BASE is not mounted (modprobe brd; mkfs.xfs /dev/ram0; mount /dev/ram0 $RAMDISK_BASE)" >&2
		exit 1
	fi
	rm -rf "$RAMDISK_BASE"/{root,lmdb}
	mkdir -p "$RAMDISK_BASE"/{root,lmdb}
	RAMDISK_DIR="$BUILD_DIR/dev/rgw/$STORE"
	mkdir -p "$RAMDISK_DIR"
	for sub in root lmdb; do
		rm -rf "$RAMDISK_DIR/$sub"
		ln -sfn "$RAMDISK_BASE/$sub" "$RAMDISK_DIR/$sub"
	done
	echo "==> ramdisk: data on $RAMDISK_BASE/{root,lmdb}"
fi

if [[ -n "$DATA_ROOT" ]]; then
	# Only the data root moves.  The LMDB cache and the userdb stay in
	# the build directory:  they are RGW's own, they do not need to be
	# wherever the objects are, and keeping them avoids re-bootstrapping
	# the test users.
	#
	# Redirected by patching ceph.conf after vstart has written it, the
	# way --gpfs does, and *not* by symlinking the build-dir path:
	# vstart.sh runs with -n and removes $dev/rgw/$STORE/root outright
	# (vstart.sh:990, :1010), so a symlink made before it ran was gone
	# by the time the daemon started and the redirect silently had no
	# effect at all.
	mkdir -p "$DATA_ROOT/root"
	echo "==> data-root: objects on $DATA_ROOT/root (conf patched after vstart)"
fi

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

	rm -rf "$GPFS_ROOT"/{root,lmdb,userdb}/* 2>/dev/null || true
fi

# --- run vstart.sh (bootstraps users + config) ---

echo "==> running vstart.sh --rgw_store $STORE"

VSTART_OPTS=(
	-o "rgw_${STORE}_cache_max_buckets=500"
	-o 'rgw_multipart_min_part_size=32'
	# DELETE /admin/driver/hint;  dev-level and off by default, so a
	# development cluster has to ask for it
	-o 'rgw_driver_debug_apis=true'
)

if $LIFECYCLE; then
	VSTART_OPTS+=(-o 'rgw_lc_debug_interval=10')

if [[ -n "$BUCKET_PROFILE" ]]; then
	case "$BUCKET_PROFILE" in
		shared|strong) ;;
		*) echo "error: --bucket-profile must be shared or strong" >&2
		   exit 1 ;;
	esac
	VSTART_OPTS+=(-o "rgw_nsfs_bucket_profile=$BUCKET_PROFILE")
fi

fi

if $INOTIFY; then
	VSTART_OPTS+=(-o "rgw_${STORE}_inotify=true")
fi

if $VAULT; then
	VSTART_OPTS+=(
		-o 'rgw_crypt_s3_kms_backend=vault'
		-o 'rgw_crypt_vault_auth=token'
		-o "rgw_crypt_vault_addr=http://127.0.0.1:8200"
		-o "rgw_crypt_vault_token_file=$BUILD_DIR/vault-token"
		-o 'rgw_crypt_vault_secret_engine=transit'
		-o 'rgw_crypt_vault_prefix=/v1/transit/'
		-o 'rgw_crypt_sse_s3_backend=vault'
		-o 'rgw_crypt_sse_s3_vault_auth=token'
		-o "rgw_crypt_sse_s3_vault_addr=http://127.0.0.1:8200"
		-o "rgw_crypt_sse_s3_vault_token_file=$BUILD_DIR/vault-token"
		-o 'rgw_crypt_sse_s3_vault_secret_engine=transit'
		-o 'rgw_crypt_sse_s3_vault_prefix=/v1/transit/'
	)
fi

VSTART_FLAGS=(-n -d)
if $PROFILE; then
	VSTART_FLAGS=(-n)
	DEBUG_RGW=0
fi

MON=0 OSD=0 MDS=0 MGR=0 RGW=1 \
	"$SRC_DIR/src/vstart.sh" "${VSTART_FLAGS[@]}" \
	--rgw_store "$STORE" \
	"${VSTART_OPTS[@]}" \
	${EXTRA_OPTS[@]+"${EXTRA_OPTS[@]}"}

# --- for non-GPFS, vstart already started the daemon ---

DATA_DIR="$BUILD_DIR/dev/rgw/$STORE/root"

if $GPFS; then
	DATA_DIR="$GPFS_ROOT/root"
elif [[ -n "$DATA_ROOT" ]]; then
	DATA_DIR="$DATA_ROOT/root"
fi

if ! $GPFS && ! $PERF; then
	# plain non-GPFS: vstart already started the daemon with defaults
	true
else

# --- kill vstart daemon, patch ceph.conf if needed, restart ---
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
VSTART_PID="$(cat "$PID" 2>/dev/null)"
if [[ -n "$VSTART_PID" ]]; then
	kill "$VSTART_PID" 2>/dev/null || true
	for i in $(seq 1 15); do
		kill -0 "$VSTART_PID" 2>/dev/null || break
		sleep 1
	done
fi

if $GPFS; then
	echo "==> patching ceph.conf for GPFS base path"
	sed -i "s|rgw nsfs base path = .*|rgw nsfs base path = $GPFS_ROOT/root|" "$CONF"
elif [[ -n "$DATA_ROOT" ]]; then
	echo "==> patching ceph.conf for --data-root"
	sed -i "s|rgw $STORE base path = .*|rgw $STORE base path = $DATA_ROOT/root|" "$CONF"
	grep -q "rgw $STORE base path = $DATA_ROOT/root" "$CONF" || {
		echo "error: could not redirect 'rgw $STORE base path' in $CONF" >&2
		exit 1
	}
fi

patch_conf_bool() {
	local key="$1" val="$2"
	if grep -q "$key" "$CONF"; then
		sed -i "s/$key = .*/$key = $val/" "$CONF"
	else
		sed -i "/rgw nsfs base path/a\\        $key = $val" "$CONF"
	fi
	echo "    $key = $val"
}

if $CLONE; then
	patch_conf_bool "rgw nsfs gpfs clone files" "true"
fi

if $LWE; then
	patch_conf_bool "rgw nsfs gpfs lwe locking" "true"
fi

start_radosgw

fi  # end GPFS block

# --- generate s3tests.conf from SAMPLE ---

SAMPLE="$SRC_DIR/qa/workunits/rgw/s3tests-rs/s3tests.conf.SAMPLE"
S3CONF="$BUILD_DIR/s3tests.conf"

if [[ -f "$SAMPLE" ]]; then
	echo "==> generating $S3CONF"
	cp "$SAMPLE" "$S3CONF"
	sed -i "s/bucket prefix = yournamehere-/bucket prefix = $(whoami)-/" "$S3CONF"
else
	echo "warning: $SAMPLE not found, skipping s3tests.conf generation" >&2
fi

export S3TEST_CONF="$S3CONF"

# --- optional sidecars ---

if $KEYCLOAK; then
	echo "==> starting Keycloak sidecar"
	S3TEST_CONF="$S3CONF" "$SRC_DIR/src/script/rgw/keycloak-vstart.sh"
fi

if $KAFKA; then
	echo "==> starting Kafka sidecar"
	S3TEST_CONF="$S3CONF" "$SRC_DIR/src/script/rgw/kafka-vstart.sh"
fi

if $LIFECYCLE; then
	if [[ -f "$S3CONF" ]]; then
		sed -i 's/^#lc_debug_interval = .*/lc_debug_interval = 10/' "$S3CONF"
	fi
fi

if $VAULT; then
	echo "==> starting Vault sidecar"
	"$SRC_DIR/src/script/rgw/vault-vstart.sh"

	# inject kms_keyid values into [s3 main] section
	if [[ -f "$S3CONF" ]]; then
		sed -i 's/^#kms_keyid = .*/kms_keyid = testkey-1/' "$S3CONF"
		if ! grep -q '^kms_keyid2' "$S3CONF"; then
			sed -i '/^kms_keyid = /a kms_keyid2 = testkey-2' "$S3CONF"
		fi
	fi
fi

# --- status + what-next output ---

S3TESTS_DIR="$SRC_DIR/qa/workunits/rgw/s3tests-rs"
FEATURE_FLAG="fails_on_nsfs"
if [[ "$STORE" == "posix" ]]; then
	FEATURE_FLAG="fails_on_posix"
fi
if $VAULT; then
	FEATURE_FLAG="${FEATURE_FLAG},has_vault"
fi

echo ""
echo "==> radosgw up on port 8000 ($STORE, data on $DATA_DIR)"
echo "    s3tests.conf: $S3CONF"
if $LIFECYCLE; then
	echo "    Lifecycle: rgw_lc_debug_interval=10"
fi
if $KEYCLOAK; then
	echo "    Keycloak: http://localhost:8080/realms/demorealm (user: testuser / testuser)"
fi
if $KAFKA; then
	echo "    Kafka: localhost:9092 (endpoint: kafka://localhost:9092)"
fi
if $VAULT; then
	echo "    Vault: http://localhost:8200 (transit keys: testkey-1, testkey-2)"
fi
echo ""
echo "To run tests:"
echo "  cd $S3TESTS_DIR"
echo "  S3TEST_CONF=$S3CONF \\"
echo "    cargo nextest run -P all --test-threads=1 --features $FEATURE_FLAG"
echo ""
echo "To stop:"
echo "  kill \$(cat $PID)"
if $KEYCLOAK; then
	echo "  $SRC_DIR/src/script/rgw/keycloak-vstart.sh --stop"
fi
if $KAFKA; then
	echo "  $SRC_DIR/src/script/rgw/kafka-vstart.sh --stop"
fi
if $VAULT; then
	echo "  $SRC_DIR/src/script/rgw/vault-vstart.sh --stop"
fi
