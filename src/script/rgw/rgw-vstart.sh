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
#   --with-impersonation
#                       Serve each request as its own POSIX identity.
#                       Creates the fixture users and groups, seeds a
#                       bucket whose objects are gated on group
#                       membership, and applies CAP_SETUID/CAP_SETGID
#                       to the built radosgw.  Needs sudo;  each step
#                       is skipped if already done.
#   --with-inotify      Enable inotify watcher on bucket directories
#                       (for sideloaded file detection; off by default)
#   --ramdisk           Mount tmpfs on data root and LMDB cache dirs
#                       (eliminates I/O latency for test runs)
#   --data-root DIR     Redirect data root and LMDB cache to DIR
#                       (e.g. --data-root /mnt/nvme for fast storage)
#   --perf              Performance tuning: NUMA pinning, tcmalloc
#                       thread cache, tcp_nodelay, no request timeout
#   -o 'key=value'      Pass extra config option through to vstart.sh
#                       (e.g. -o 'rgw_posix_sync_policy=relaxed')
#   --profile           Start without debug logging (no -d to vstart.sh)
#                       for performance profiling; sets all debug to 0
#   --foreground        Print the radosgw command instead of running it;
#                       use this to start the daemon as root in another
#                       terminal (LWE requires root for DMAPI handles)
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
IMPERSONATE=false
IMPERSONATE_CLEAN=false
INOTIFY=false
RAMDISK=false
DATA_ROOT=""
PERF=false
PROFILE=false
GPFS_ROOT=/mnt/rgw/nsfs
DEBUG_RGW=20
FOREGROUND=false
EXTRA_OPTS=()

while [[ $# -gt 0 ]]; do
	case "$1" in
		--store)      STORE="$2"; shift 2 ;;
		--gpfs)       GPFS=true; shift ;;
		--clone)      CLONE=true; GPFS=true; shift ;;
		--lwe)        LWE=true; GPFS=true; shift ;;
		--clean)      CLEAN=true; IMPERSONATE_CLEAN=true; shift ;;
		--with-keycloak) KEYCLOAK=true; shift ;;
		--with-kafka) KAFKA=true; shift ;;
		--with-vault) VAULT=true; shift ;;
		--with-lifecycle) LIFECYCLE=true; shift ;;
		--with-impersonation) IMPERSONATE=true; shift ;;
		--with-inotify) INOTIFY=true; shift ;;
		--ramdisk) RAMDISK=true; shift ;;
		--data-root) DATA_ROOT="$2"; shift 2 ;;
		--perf) PERF=true; shift ;;
		--profile) PROFILE=true; shift ;;
		--gpfs-root)  GPFS_ROOT="$2"; shift 2 ;;
		--debug-rgw)  DEBUG_RGW="$2"; shift 2 ;;
		--foreground) FOREGROUND=true; shift ;;
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

if $CLEAN; then
	echo "==> --clean: wiping config DB and all data"
	rm -f "$BUILD_DIR/dev/rgw/dbstore/config.db" 2>/dev/null || true
fi

echo "==> cleaning data dirs"
rm -rf "$BUILD_DIR/dev/rgw/$STORE"/{lmdb,root,userdb}/* 2>/dev/null || true

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
	if [[ ! -d "$DATA_ROOT" ]]; then
		echo "error: --data-root $DATA_ROOT does not exist" >&2
		exit 1
	fi
	mkdir -p "$DATA_ROOT"/{root,lmdb}
	DATA_DIR="$BUILD_DIR/dev/rgw/$STORE"
	mkdir -p "$DATA_DIR"
	for sub in root lmdb; do
		rm -rf "$DATA_DIR/$sub"
		ln -sfn "$DATA_ROOT/$sub" "$DATA_DIR/$sub"
	done
	echo "==> data-root: data on $DATA_ROOT/{root,lmdb}"
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
fi

if $IMPERSONATE; then
	VSTART_OPTS+=(-o 'rgw_nsfs_impersonate=true')
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

# --- impersonation:  users, groups and the capability ------------
#
# Before vstart, because the capability has to be on the binary
# before the daemon execs it.  The fixture tree cannot go here --
# vstart removes $nsfs_dir/root (vstart.sh:1010) -- so it is seeded
# further down, once the root exists again.

IMP_GRP_NAME=rgwfix
IMP_GRP_GID=70010
IMP_USER_A=rgwalice
IMP_UID_A=70101
IMP_USER_B=rgwbob
IMP_UID_B=70102
IMP_BUCKET=impersonate-fixture

if $IMPERSONATE; then
	if [[ "$STORE" != "nsfs" ]]; then
		echo "error: --with-impersonation only applies to --store nsfs" >&2
		exit 1
	fi

	echo "==> --with-impersonation: users, groups and capability (needs sudo)"

	# Objects written under impersonation belong to the identity
	# that wrote them, so the developer cannot chmod them and
	# vstart's own clean cannot replace them.  Remove the data
	# root here, while sudo is already in hand, rather than
	# leaving vstart to trip over it.  (The unlink itself would
	# succeed -- bucket directories are gateway-owned, and a
	# directory's owner may unlink anything inside it -- but the
	# fixture's chmod and chown still need root.)
	if $IMPERSONATE_CLEAN; then
		if $GPFS; then
			sudo rm -rf "$GPFS_ROOT/root"
		else
			sudo rm -rf "$BUILD_DIR/dev/rgw/$STORE/root"
		fi
	fi

	getent group "$IMP_GRP_NAME" >/dev/null 2>&1 || \
		sudo groupadd -g "$IMP_GRP_GID" "$IMP_GRP_NAME"
	# alice holds the gating group, bob deliberately does not
	getent passwd "$IMP_USER_A" >/dev/null 2>&1 || \
		sudo useradd -u "$IMP_UID_A" -M -N -s /sbin/nologin \
			-G "$IMP_GRP_NAME" "$IMP_USER_A"
	getent passwd "$IMP_USER_B" >/dev/null 2>&1 || \
		sudo useradd -u "$IMP_UID_B" -M -N -s /sbin/nologin \
			"$IMP_USER_B"

	# ninja clears file capabilities on every relink, so this is
	# reapplied on each start rather than being one-time setup.
	#
	# CAP_DAC_READ_SEARCH alongside the two identity capabilities:
	# objects written under impersonation belong to the identity
	# that wrote them, and the gateway still has to read their
	# attributes to build listing rows.  Listings are complete and
	# unfiltered by design -- an object's contents are protected by
	# filesystem permissions, its name is not.
	#
	# It is safe only because the registration bracket clears the
	# effective capability set:  a personality copies the
	# registering task's credentials whole, so without that clear
	# every impersonated read would inherit this and silently
	# bypass DAC.  Measured in probes/results-capprobe-2026-10-01.txt.
	sudo setcap cap_dac_read_search,cap_setuid,cap_setgid+ep \
		"$BUILD_DIR/bin/radosgw"

	# Every thread that serves an impersonated request creates an
	# io_uring ring, because the open carries the personality on a
	# submission entry, and ring memory is charged against
	# RLIMIT_MEMLOCK.  One limit against the whole frontend thread
	# count:  a thread that cannot create a ring cannot register a
	# personality, and its requests fail with a 500 rather than being
	# served as the gateway.
	#
	# Control rings are 8 entries, so the default 8 MiB is ample.  The
	# check is here because a deployment that turns the io_uring data
	# engine on makes them 1024, and then a few hundred threads will
	# not fit.
	MEMLOCK_KB=$(ulimit -l)
	if [ "$MEMLOCK_KB" != "unlimited" ] && [ "$MEMLOCK_KB" -lt 65536 ]; then
		echo "==> note: RLIMIT_MEMLOCK is ${MEMLOCK_KB}K." \
			"Ample for control rings;  raise it before enabling" \
			"the io_uring data engine on a large frontend" >&2
	fi
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

# --- build the radosgw command line ---

BEAST_OPTS="beast port=8000"
RGW_PREFIX=()

if $PERF; then
	BEAST_OPTS="beast port=8000 request_timeout_ms=0 tcp_nodelay=1"
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
	--log-file="$BUILD_DIR/out/radosgw.8000.log"
	--admin-socket="$BUILD_DIR/out/radosgw.8000.asok"
	--pid-file="$PID"
	--rgw_luarocks_location="$BUILD_DIR/out/radosgw.8000.luarocks"
	--debug-rgw="$DEBUG_RGW"
	--debug-ms=0
	-n client.rgw.8000
	--rgw_frontends="$BEAST_OPTS"
)

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
	echo "data root: $GPFS_ROOT/root"
	exit 0
fi

echo "==> starting radosgw"
"${RGW_CMD[@]}"

sleep 2
if ! fuser 8000/tcp >/dev/null 2>&1; then
	echo "error: radosgw did not start" >&2
	tail -20 "$BUILD_DIR/out/radosgw.8000.log"
	exit 1
fi

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

# --- impersonation:  the fixture ---------------------------------
#
# The bucket is created over S3 rather than placed in the data root.
# An unmarked directory has no owner RGW can resolve -- a base
# bucket's owner waits on the account import -- so a sideloaded one
# is refused with AccessDenied before any permission question is
# reached.  Creating it through the gateway gives it an owner;  only
# the ownership and mode of the gated object need root.
#
# That object ends up mode 0040 owned root:$IMP_GRP_NAME.  No owner
# bits and no other bits, so holding the group is the only thing
# that can grant a read -- credprobe's shape, and what makes a
# denial a control rather than an absence.

if $IMPERSONATE; then
	# vstart's own user.  Taken from the pair radosgw creates at
	# first start (driver/posix/posixDB.cc) rather than from
	# s3tests.conf, which is not generated in every tree.
	IMP_AK=0555b35654ad1656d804
	IMP_SK='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

	# The data root stays as the gateway made it.  Bucket
	# directories are owned by the gateway and created sticky and
	# writable, so identities create objects inside them without
	# needing to reach them by path -- an impersonated open starts
	# from the descriptor the gateway already holds.

	# testid is bound to the fixture uid so the seeded objects are
	# owned by a real identity rather than by the gateway -- and so
	# that the writes are not refused, since with impersonation on
	# an identity with no record cannot be served.
	if ! python3 "$SRC_DIR/src/script/rgw/nsfs-impersonate-fixture.py" \
			localhost 8000 "$IMP_AK" "$IMP_SK" "$IMP_BUCKET" \
			testid "$IMP_UID_A"; then
		echo "error: could not create the impersonation fixture" >&2
		exit 1
	fi

	IMP_DIR="$DATA_DIR/$IMP_BUCKET"
	if [[ ! -f "$IMP_DIR/grouped.txt" ]]; then
		echo "error: $IMP_DIR/grouped.txt absent after creation;" \
			"is $DATA_DIR the data root?" >&2
		exit 1
	fi

	# Everything here now belongs to the identity that wrote it,
	# so the developer cannot chmod any of it -- sudo throughout.
	#
	# Bucket directories under impersonation are gateway-owned and
	# 1777:  writable, so identities can create objects in them.
	#
	# The sticky bit is not load-bearing.  A directory's owner may
	# unlink anything inside it regardless, and the gateway owns
	# every bucket directory -- so sticky does not stop one
	# identity's object being removed on behalf of another, and
	# deletion deliberately does not run under the requester's
	# identity.  It is kept as a true statement about the
	# filesystem rather than about RGW:  an identity reaching this
	# directory by path, outside the gateway, cannot unlink
	# another's objects.
	#
	# Asserted rather than set, so a change to that decision shows
	# up here rather than silently.
	case "$(stat -c %a "$IMP_DIR")" in
		1777) ;;
		*) echo "warning: $IMP_BUCKET is $(stat -c %a "$IMP_DIR")," \
			"expected 1777" >&2 ;;
	esac

	# the control:  readable by anyone, so a test that cannot read
	# this has a broken gateway rather than a working denial
	sudo chmod 0644 "$IMP_DIR/open.txt"

	sudo chown "root:$IMP_GRP_NAME" "$IMP_DIR/grouped.txt"
	sudo chmod 0040 "$IMP_DIR/grouped.txt"

	echo "==> --with-impersonation: $IMP_BUCKET seeded;" \
		"grouped.txt is 0040 root:$IMP_GRP_NAME"
fi

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
if $IMPERSONATE; then
	echo "    Impersonation: on;  bucket $IMP_BUCKET has open.txt (0644)" \
		"and grouped.txt (0040 root:$IMP_GRP_NAME, gid $IMP_GRP_GID)"
	echo "    Fixture ids: $IMP_USER_A=$IMP_UID_A in $IMP_GRP_NAME," \
		"$IMP_USER_B=$IMP_UID_B not"
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
