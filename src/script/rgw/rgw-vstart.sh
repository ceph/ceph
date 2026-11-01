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
#   --port N            The port radosgw serves on (default: 8000);  the
#                       one s3tests.conf names
#   --with-keycloak     Start a local Keycloak container for WebIdentity
#                       testing (requires podman, jq, openssl, curl)
#   --with-kafka        Start a local Kafka broker for notification testing
#                       (requires podman)
#   --with-vault        Start a local HashiCorp Vault for SSE-KMS / SSE-S3
#                       testing (requires podman, curl)
#   --with-lifecycle     Set rgw_lc_debug_interval=10 so lifecycle
#                       expiration tests complete in seconds, and
#                       uncomment lc_debug_interval in s3tests.conf
#   --with-non-md5-etag Set rgw_non_md5_etag=true, so the etag is the
#                       mtime-ino string the version ids use rather
#                       than a content digest.  Off by default, and
#                       the only way the suite can reach that path
#   --with-impersonation
#                       Serve each request as its own POSIX identity.
#                       Creates the fixture users and groups, seeds a
#                       bucket whose objects are gated on group
#                       membership, and applies the six capabilities
#                       (n) needs to the built radosgw -- three held,
#                       three raised around named sites.  Needs sudo;  each step
#                       is skipped if already done.  Also makes two
#                       unplaced, account-owned directories for
#                       placement tests, afresh on every start.
#                       Exits with an error if run as root.
#   --no-fixture        With --with-impersonation:  the users, groups and
#                       capabilities only -- no seeded bucket, no
#                       unplaced directories.  For a --data-root another
#                       gateway wrote, which the fixture would write into.
#   --with-inotify      Enable inotify watcher on bucket directories
#                       (for sideloaded file detection; off by default)
#   --ramdisk           Mount tmpfs on data root and LMDB cache dirs
#                       (eliminates I/O latency for test runs)
#   --noobaa-config-root DIR
#                       Read NooBaa's configuration store from DIR.  Needed
#                       to adopt a NooBaa deployment;  without it the
#                       adoption mode is inert.
#   --adoption-mode M   command | automatic.  The driver defaults to
#                       command, which adopts nothing until asked through
#                       Admin Ops (the adoption driver hint, or a profile
#                       change out of base);  automatic adopts the whole
#                       store at startup.
#   --bucket-profile P  shared | strong:  the profile new nsfs buckets are
#                       marked with.  The driver defaults to shared;  FSIO
#                       (librgw, the librgw_file tests) needs strong.
#   --access-model M    posix-strict | s3-native:  what nsfs
#                       does when S3 authorises an operation the filesystem
#                       denies the requester (rgw_nsfs_access_model).  The
#                       driver defaults to posix-strict.
#   --user-identity-override
#                       An account user's own identity row overrides its
#                       account's (rgw_nsfs_user_identity_override), as
#                       NooBaa serves an IAM user from its own record.
#                       The driver defaults to off.
#   --role-identity-override
#                       A role given an identity row runs as it, not as its
#                       account (rgw_nsfs_role_identity_override).  The
#                       driver defaults to off.
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
#                       (patched into ceph.conf), --debug-rgw, --perf and
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
NON_MD5_ETAG=false
IMPERSONATE=false
IMPERSONATE_CLEAN=false
NO_FIXTURE=false
INOTIFY=false
RAMDISK=false
DATA_ROOT=""
NOOBAA_CONFIG_ROOT=""
ADOPTION_MODE=""
BUCKET_PROFILE=""
ACCESS_MODEL=""
USER_IDENTITY_OVERRIDE=false
ROLE_IDENTITY_OVERRIDE=false
PERF=false
PROFILE=false
GPFS_ROOT=/mnt/rgw/nsfs
DEBUG_RGW=20
RGW_PORT=8000
FOREGROUND=false
RESTART=false
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
		--with-non-md5-etag) NON_MD5_ETAG=true; shift ;;
		--with-impersonation) IMPERSONATE=true; shift ;;
		--no-fixture) NO_FIXTURE=true; shift ;;
		--with-inotify) INOTIFY=true; shift ;;
		--ramdisk) RAMDISK=true; shift ;;
		--data-root) DATA_ROOT="$2"; shift 2 ;;
		--noobaa-config-root) NOOBAA_CONFIG_ROOT="$2"; shift 2 ;;
		--adoption-mode) ADOPTION_MODE="$2"; shift 2 ;;
		--bucket-profile) BUCKET_PROFILE="$2"; shift 2 ;;
		--access-model) ACCESS_MODEL="$2"; shift 2 ;;
		--user-identity-override) USER_IDENTITY_OVERRIDE=true; shift ;;
		--role-identity-override) ROLE_IDENTITY_OVERRIDE=true; shift ;;
		--perf) PERF=true; shift ;;
		--profile) PROFILE=true; shift ;;
		--gpfs-root)  GPFS_ROOT="$2"; shift 2 ;;
		--debug-rgw)  DEBUG_RGW="$2"; shift 2 ;;
		--port)       RGW_PORT="$2"; shift 2 ;;
		--foreground) FOREGROUND=true; shift ;;
		--restart) RESTART=true; shift ;;
		-o) EXTRA_OPTS+=(-o "$2"); shift 2 ;;
		*) echo "unknown option: $1" >&2; exit 1 ;;
	esac
done

if ! [[ "$RGW_PORT" =~ ^[0-9]+$ ]]; then
	echo "error: --port takes a port number" >&2
	exit 1
fi

if [[ "$STORE" != "nsfs" && "$STORE" != "posix" ]]; then
	echo "error: --store must be nsfs or posix" >&2
	exit 1
fi

if $GPFS && [[ "$STORE" != "nsfs" ]]; then
	echo "error: --gpfs only applies to --store nsfs" >&2
	exit 1
fi

# Root holds every capability, and they override the file modes that
# impersonation exists to enforce:  every result in this area comes
# out the same whoever the request runs as.  The gateway must run as
# an ordinary user holding only its file capabilities.  The script
# raises sudo itself for the steps that need it.
if $IMPERSONATE && [[ $EUID -eq 0 ]]; then
	echo "error: --with-impersonation must not run as root:  root's" \
		"capabilities override the permissions it enforces.  Run" \
		"as yourself;  the script uses sudo where it must." >&2
	exit 1
fi

BUILD_DIR="$(pwd)"
if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
	echo "error: run this from the build directory" >&2
	exit 1
fi

SRC_DIR="$(grep -m1 'CMAKE_HOME_DIRECTORY' CMakeCache.txt | cut -d= -f2)"
CONF="$BUILD_DIR/ceph.conf"
PID="$BUILD_DIR/out/radosgw.$RGW_PORT.pid"

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
	BEAST_OPTS="beast port=$RGW_PORT"
	RGW_PREFIX=()

	if $PERF; then
		BEAST_OPTS="beast port=$RGW_PORT request_timeout_ms=0 tcp_nodelay=1"
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
		--log-file="$BUILD_DIR/out/radosgw.$RGW_PORT.log"
		--admin-socket="$BUILD_DIR/out/radosgw.$RGW_PORT.asok"
		--pid-file="$PID"
		--rgw_luarocks_location="$BUILD_DIR/out/radosgw.$RGW_PORT.luarocks"
		--debug-rgw="$DEBUG_RGW"
		--debug-ms=0
		-n "client.rgw.$RGW_PORT"
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

	radosgw_serves
}

# The radosgw started here is the one serving:  its pid file names a
# live process, and the port is listened on.  A listener alone is not
# enough -- a gateway from an earlier run, root's say, answers for one
# that failed to bind and exited.
radosgw_serves() {
	sleep 2
	local pid
	pid="$(cat "$PID" 2>/dev/null || true)"
	if [[ -z "$pid" || ! -d "/proc/$pid" ]]; then
		echo "error: radosgw did not start" >&2
		tail -20 "$BUILD_DIR/out/radosgw.$RGW_PORT.log"
		if ss -ltnH "sport = :$RGW_PORT" | grep -q .; then
			echo "error: something else holds port $RGW_PORT;  stop it, then run again" >&2
		fi
		exit 1
	fi
	# ss, not fuser:  a radosgw given file capabilities is not dumpable, so
	# its /proc/<pid>/fd is root's and fuser cannot see the socket.
	if ! ss -ltnH "sport = :$RGW_PORT" | grep -q .; then
		echo "error: radosgw (pid $pid) is not listening on port $RGW_PORT" >&2
		tail -20 "$BUILD_DIR/out/radosgw.$RGW_PORT.log"
		exit 1
	fi
}

# What --with-impersonation needs of the machine, checked before the
# running gateway is stopped, so a refusal leaves it as it was.
if $IMPERSONATE; then
	# setcap on a nosuid mount succeeds and the kernel then ignores
	# the capabilities, so every impersonated request would fail as a
	# denial.  Refused here, before anything is started.
	if findmnt -no OPTIONS -T "$BUILD_DIR/bin" | tr , '\n' | grep -qx nosuid; then
		echo "error: $BUILD_DIR/bin is on a nosuid mount, where file" \
			"capabilities are ignored;  --with-impersonation needs a" \
			"build on a mount without nosuid" >&2
		exit 1
	fi

	# The fixture uids own none of the directories above the data
	# root and are in none of their groups, so each needs the
	# other-execute bit to reach a bucket;  a 0700 home directory is
	# the usual one that lacks it.  The first existing ancestor of the
	# data root without it is named, rather than surfacing later as
	# denials.  Not the data root itself:  in an adopted tree it is
	# NooBaa's, an account's at 0770, and what is below it is the
	# access model's.
	if $GPFS; then
		_root="$GPFS_ROOT/root"
	elif [[ -n "$DATA_ROOT" ]]; then
		_root="$DATA_ROOT/root"
	else
		_root="$BUILD_DIR/dev/rgw/$STORE/root"
	fi
	_d="$(dirname "$_root")"
	while [[ "$_d" != "/" ]]; do
		if [[ -d "$_d" ]] && (( ( 8#$(stat -c '%a' "$_d") & 1 ) == 0 )); then
			echo "error: the fixture uids cannot search $_d, so they cannot" \
				"reach the data root $_root.  Give it o+x, or use a" \
				"build or --data-root under a directory every user can" \
				"search." >&2
			exit 1
		fi
		_d="$(dirname "$_d")"
	done
fi

# --- --restart:  the running gateway again, on the same state ---

if $RESTART; then
	if $GPFS || $CLEAN || $KEYCLOAK || $KAFKA || $VAULT || $LIFECYCLE || \
		$NON_MD5_ETAG || $NO_FIXTURE || $INOTIFY || $RAMDISK || $PROFILE || \
		$FOREGROUND || $USER_IDENTITY_OVERRIDE || $ROLE_IDENTITY_OVERRIDE || \
		[[ -n "$DATA_ROOT" ]] || \
		[[ -n "$NOOBAA_CONFIG_ROOT" ]] || [[ -n "$BUCKET_PROFILE" ]] || \
		[[ -n "$ACCESS_MODEL" ]] || (( ${#EXTRA_OPTS[@]} )); then
		echo "error: --restart takes only --store, --adoption-mode," \
			"--debug-rgw, --port, --perf and --with-impersonation;  the rest is" \
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
	echo "==> radosgw restarted on port $RGW_PORT;  nothing cleaned"
	exit 0
fi

# --- kill any existing radosgw ---

# Every gateway this build directory runs, on whatever port:  they share
# its state, which --clean wipes.
echo "==> killing any existing radosgw of this build, and any on port $RGW_PORT"
for pidfile in "$BUILD_DIR"/out/radosgw.*.pid; do
	[[ -f "$pidfile" ]] || continue
	kill "$(cat "$pidfile" 2>/dev/null)" 2>/dev/null || true
	sleep 1
done
fuser -k "$RGW_PORT/tcp" 2>/dev/null || true

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
		# An impersonated run leaves entries other uids own -- every
		# bucket directory is its creator's -- which the developer
		# cannot remove;  vstart.sh's own clean then fails.  Removed
		# with sudo, which prompts where it must, as the fixture's
		# chmod does;  refused only where nothing can answer a prompt.
		if compgen -G "$d/*" >/dev/null; then
			if sudo -n true 2>/dev/null || [[ -t 0 ]]; then
				echo "==> removing entries other uids own in $d (sudo)"
				sudo rm -rf "${d:?}"/*
			else
				echo "error: $d holds entries other uids own, left by a" >&2
				echo "       --with-impersonation run.  Remove them, then run again:" >&2
				echo "         sudo rm -rf $d" >&2
				exit 1
			fi
		fi
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
fi

if $NON_MD5_ETAG; then
	VSTART_OPTS+=(-o 'rgw_non_md5_etag=true')
fi

if [[ -n "$BUCKET_PROFILE" ]]; then
	case "$BUCKET_PROFILE" in
		shared|strong) ;;
		*) echo "error: --bucket-profile must be shared or strong" >&2
		   exit 1 ;;
	esac
	VSTART_OPTS+=(-o "rgw_nsfs_bucket_profile=$BUCKET_PROFILE")
fi

if [[ -n "$ACCESS_MODEL" ]]; then
	case "$ACCESS_MODEL" in
		posix-strict|s3-native) ;;
		*) echo "error: --access-model must be posix-strict" \
			"or s3-native" >&2
		   exit 1 ;;
	esac
	VSTART_OPTS+=(-o "rgw_nsfs_access_model=$ACCESS_MODEL")
fi

if $USER_IDENTITY_OVERRIDE; then
	VSTART_OPTS+=(-o "rgw_nsfs_user_identity_override=true")
fi

if $ROLE_IDENTITY_OVERRIDE; then
	VSTART_OPTS+=(-o "rgw_nsfs_role_identity_override=true")
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
# the POSIX identity an unauthenticated request runs as.  A deployment
# that serves public buckets has an anonymous account
# (ACCOUNT_IMPORT.md 6.8;  NooBaa provisions one with
# `account add --anonymous`), and with impersonation on a request whose
# identity has no record is denied rather than served as the gateway.
# Its own uid, not the gateway's:  anonymous is then a distinct
# principal on the filesystem, which is what lets a test assert it
# cannot reach what alice can.
IMP_USER_N=rgwanon
IMP_UID_N=70103
# the suite's *tenanted* user is a principal like any other, and under
# impersonation a principal with no identity row cannot be served -- so
# the cors `_tenant` tests failed with "no POSIX identity recorded for
# testx$..." and were measuring that rather than CORS (2026-10-16).
IMP_USER_T=rgwtenant
IMP_UID_T=70104
# The rest of the suite's principals.  Every identity that can
# authenticate needs a record, and this list has fallen behind that
# three times -- the tenanted user above (2026-10-16), anonymous, and
# then these four (2026-10-19), each found by an unrelated test failing
# with "no POSIX identity recorded for ..." and measuring that instead
# of what it was written for.  `identity::test_every_principal_is_
# provisioned` now fails by name when it happens again.
#
# The two IAM root accounts are here because an account authenticates
# like any other principal, and a user created inside an account needs
# no row of its own:  resolve_credentials() tries the user's row and
# falls back to its account's when that is absent
# (identity_db.cc:893), which is what makes these two rows cover every
# user of either account.
IMP_USER_I=rgwiam
IMP_UID_I=70105
IMP_USER_P=rgwpolicy
IMP_UID_P=70106
IMP_USER_R1=rgwacct1
IMP_UID_R1=70107
IMP_USER_R2=rgwacct2
IMP_UID_R2=70108
# I9's users:  the stand-up's `copy` step moves an account from the first
# to the second after making a user in it, and the suite's override tests
# bind them.  Their own, so no other test's rows carry their uids --
# rows sharing a uid that disagree on groups make a file's credential
# unresolvable (complete_file_credentials()) -- and in no gating group,
# so nothing about them depends on rgwfix.
IMP_USER_CA=rgwcopya
IMP_UID_CA=70109
IMP_USER_CB=rgwcopyb
IMP_UID_CB=70110
IMP_BUCKET=impersonate-fixture
IMP_OWNED=impersonate-owned
IMP_MULTI=impersonate-multiproto
IMP_UID_NFS=70103

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
	# and their memberships, which the dynamic-groups tests and the
	# stand-up's groups step change and put back:  a run interrupted
	# between the two would otherwise leave them changed for every
	# run after
	id -nG "$IMP_USER_A" | grep -qw "$IMP_GRP_NAME" || \
		sudo gpasswd -a "$IMP_USER_A" "$IMP_GRP_NAME" >/dev/null
	! id -nG "$IMP_USER_B" | grep -qw "$IMP_GRP_NAME" || \
		sudo gpasswd -d "$IMP_USER_B" "$IMP_GRP_NAME" >/dev/null
	# in no gating group:  anonymous reaches only what is open to all
	getent passwd "$IMP_USER_N" >/dev/null 2>&1 || \
		sudo useradd -u "$IMP_UID_N" -M -N -s /sbin/nologin \
			"$IMP_USER_N"
	# the remaining principals, each in no gating group
	for _u in "$IMP_USER_I:$IMP_UID_I" "$IMP_USER_P:$IMP_UID_P" \
		  "$IMP_USER_R1:$IMP_UID_R1" "$IMP_USER_R2:$IMP_UID_R2" \
		  "$IMP_USER_CA:$IMP_UID_CA" "$IMP_USER_CB:$IMP_UID_CB"; do
		_n=${_u%%:*}
		_i=${_u##*:}
		getent passwd "$_n" >/dev/null 2>&1 || \
			sudo useradd -u "$_i" -M -N -s /sbin/nologin "$_n"
	done
	# the tenanted user, in no gating group:  what it tests is CORS
	# and presigning, not group reach
	getent passwd "$IMP_USER_T" >/dev/null 2>&1 || \
		sudo useradd -u "$IMP_UID_T" -M -N -s /sbin/nologin \
			"$IMP_USER_T"

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
	#
	# Six, and permitted-only:  +p, not +ep.
	#
	# A file capability set carries one effective flag for all of it,
	# so +ep would make every one of the six effective on every
	# thread -- including CAP_DAC_OVERRIDE on the 31 io_context_pool
	# threads, which are created before the driver initialises, so a
	# lowering at startup cannot reach them (measured 2026-10-16:
	# main 0xc4, every pool thread 0xcf).  With +p nothing starts
	# effective and each capability is put in the effective set at
	# its own site:  CAP_DAC_READ_SEARCH by the thread that reads
	# with no identity, and CAP_DAC_OVERRIDE, CAP_CHOWN, CAP_FOWNER
	# by with_capabilities() around a named call.
	sudo setcap cap_dac_read_search,cap_setuid,cap_setgid,cap_dac_override,cap_chown,cap_fowner+p \
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
	--rgw_port "$RGW_PORT" \
	"${VSTART_OPTS[@]}" \
	${EXTRA_OPTS[@]+"${EXTRA_OPTS[@]}"}

# --- for non-GPFS, vstart already started the daemon ---

DATA_DIR="$BUILD_DIR/dev/rgw/$STORE/root"

if $GPFS; then
	DATA_DIR="$GPFS_ROOT/root"
elif [[ -n "$DATA_ROOT" ]]; then
	DATA_DIR="$DATA_ROOT/root"
fi

if ! $GPFS && ! $PERF && ! $IMPERSONATE && [[ -z "$DATA_ROOT" ]]; then
	# plain non-GPFS: vstart already started the daemon with defaults.
	# Not with impersonation, whose option goes on the command line below.
	radosgw_serves
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

if [[ -n "$NOOBAA_CONFIG_ROOT" ]]; then
	if [[ ! -d "$NOOBAA_CONFIG_ROOT" ]]; then
		echo "error: --noobaa-config-root $NOOBAA_CONFIG_ROOT is not a directory" >&2
		exit 1
	fi
	patch_conf_bool "rgw nsfs noobaa config root" "$NOOBAA_CONFIG_ROOT"
fi

if [[ -n "$ADOPTION_MODE" ]]; then
	case "$ADOPTION_MODE" in
		command|automatic) ;;
		*) echo "error: --adoption-mode must be command or automatic" >&2
		   exit 1 ;;
	esac
	patch_conf_bool "rgw nsfs adoption mode" "$ADOPTION_MODE"
	if [[ -z "$NOOBAA_CONFIG_ROOT" ]]; then
		echo "    note: the mode is inert without --noobaa-config-root"
	fi
fi

if $CLONE; then
	patch_conf_bool "rgw nsfs gpfs clone files" "true"
fi

if $LWE; then
	patch_conf_bool "rgw nsfs gpfs lwe locking" "true"
fi

start_radosgw

fi  # end GPFS block

# s3tests.conf is not generated here:  the suite is its own repository,
# and its s3tests.conf.SAMPLE already carries every user vstart.sh
# creates.  Copy that once and point S3TEST_CONF at the copy.  The
# sidecars, --with-lifecycle and --with-vault below record what they set
# up in that file, when S3TEST_CONF names one.
S3CONF="${S3TEST_CONF:-}"

# --- impersonation:  the fixture ---------------------------------
#
# The bucket is created over S3 rather than placed in the data root.
# An unmarked directory has no owner RGW can resolve -- a base
# bucket's owner waits on the account import -- so a sideloaded one
# is denied (AccessDenied) before any permission question is
# reached.  Creating it through the gateway gives it an owner;  only
# the ownership and mode of the gated object need root.
#
# That object ends up mode 0040 owned root:$IMP_GRP_NAME.  No owner
# bits and no other bits, so holding the group is the only thing
# that can grant a read -- credprobe's shape, and what makes a
# denial a control rather than an absence.

if $IMPERSONATE && ! $NO_FIXTURE; then
	# vstart's own user.  Taken from the pair radosgw creates at
	# first start (driver/posix/posixDB.cc) rather than from
	# s3tests.conf, which is not generated in every tree.
	IMP_AK=0555b35654ad1656d804
	IMP_SK='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

	# The data root stays as the gateway made it.  A bucket
	# directory belongs to the identity that created the bucket, at
	# 0770 with no sticky bit, as NooBaa makes it (DECISIONS
	# 2026-10-23), so the developer cannot see inside one:  sudo for
	# every look.

	# testid is bound to the fixture uid so the seeded objects are
	# owned by a real identity rather than by the gateway -- and so
	# that the writes are not denied, since with impersonation on
	# an identity with no record cannot be served.
	# The suite's alt user is bound to the other fixture uid for the
	# whole run, so a test that reads testid's object as the alt user
	# meets a different identity rather than none.  Its id is the one
	# vstart.sh gives it.
	IMP_ALT_USER=56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234
	# single-quoted:  the id begins with a digit after the tenant's
	# `$`, which double quotes would read as a positional parameter
	IMP_TENANT_USER='testx$9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
	# vstart's own ids, as s3tests.conf records them
	IMP_IAM_USER=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
	IMP_POLICY_USER=s3policyonly
	IMP_ACCT1=RGW11111111111111111
	IMP_ACCT2=RGW22222222222222222
	if ! python3 "$SRC_DIR/src/script/rgw/nsfs-impersonate-fixture.py" \
			localhost "$RGW_PORT" "$IMP_AK" "$IMP_SK" "$IMP_BUCKET" \
			testid "$IMP_UID_A" "$IMP_ALT_USER" "$IMP_UID_B" \
			"$IMP_TENANT_USER" "$IMP_UID_T" \
			anonymous "$IMP_UID_N" \
			"$IMP_IAM_USER" "$IMP_UID_I" \
			"$IMP_POLICY_USER" "$IMP_UID_P" \
			"$IMP_ACCT1" "$IMP_UID_R1" \
			"$IMP_ACCT2" "$IMP_UID_R2"; then
		echo "error: could not create the impersonation fixture" >&2
		exit 1
	fi

	IMP_DIR="$DATA_DIR/$IMP_BUCKET"
	if ! sudo test -f "$IMP_DIR/grouped.txt"; then
		echo "error: $IMP_DIR/grouped.txt absent after creation;" \
			"is $DATA_DIR the data root?" >&2
		exit 1
	fi

	# Everything here now belongs to the identity that wrote it,
	# so the developer cannot chmod any of it -- sudo throughout.
	#
	# The bucket directory is its creator's -- testid, so
	# $IMP_UID_A -- at 0770 (raise site 4).  Asserted rather than
	# set, so a change to that decision shows up here rather than
	# silently.
	case "$(sudo stat -c '%a %u' "$IMP_DIR")" in
		"770 $IMP_UID_A") ;;
		*) echo "warning: $IMP_BUCKET is" \
			"$(sudo stat -c '%a %u' "$IMP_DIR")," \
			"expected 770 $IMP_UID_A" >&2 ;;
	esac

	# the control:  readable by anyone, so a test that cannot read
	# this has a broken gateway rather than a working denial
	sudo chmod 0644 "$IMP_DIR/open.txt"

	sudo chown "root:$IMP_GRP_NAME" "$IMP_DIR/grouped.txt"
	sudo chmod 0040 "$IMP_DIR/grouped.txt"

	echo "==> --with-impersonation: $IMP_BUCKET seeded;" \
		"grouped.txt is 0040 root:$IMP_GRP_NAME"

	# Two unplaced directories under the base path, for tests that
	# place them as buckets through /admin/nsfs/bucket.  Neither is a
	# bucket until placed, and a placed one carries nsfs state, which
	# placement then rejects, so both are made afresh on every start.
	#
	# $IMP_OWNED is what an account's bucket looks like after NooBaa
	# made it, or after adoption:  the account's uid throughout, and
	# NooBaa's modes, 0777 and 0666 masked by NSFS_UMASK 007
	# (bucketspace_fs.js create_bucket;  config.js).
	#
	# $IMP_MULTI is the same, shared with NFS through the group
	# $IMP_GRP_NAME, which the account's uid is in:  the directories
	# are setgid, so what anyone writes there takes the group.  An NFS
	# client writes as its own uid ($IMP_UID_NFS, with $IMP_GRP_NAME
	# in the group list it sends) under umask 022.  The uid has no
	# passwd entry here, as NFS client uids often have none:  the
	# server sees only the ids on the files.
	mk_tree() {	# <dir> <group> <dir mode>
		sudo rm -rf "${DATA_DIR:?}/$1"
		sudo install -d -o "$IMP_UID_A" -g "$2" -m "$3" "$DATA_DIR/$1"
		sudo install -o "$IMP_UID_A" -g "$2" -m 0660 /dev/null \
			"$DATA_DIR/$1/obj"
		sudo install -d -o "$IMP_UID_A" -g "$2" -m "$3" \
			"$DATA_DIR/$1/sub"
		sudo install -o "$IMP_UID_A" -g "$2" -m 0660 /dev/null \
			"$DATA_DIR/$1/sub/nested"
	}
	mk_tree "$IMP_OWNED" "$IMP_UID_A" 0770
	mk_tree "$IMP_MULTI" "$IMP_GRP_GID" 2770
	sudo install -o "$IMP_UID_NFS" -g "$IMP_GRP_GID" -m 0644 /dev/null \
		"$DATA_DIR/$IMP_MULTI/nfs-written"
	sudo install -d -o "$IMP_UID_NFS" -g "$IMP_GRP_GID" -m 2755 \
		"$DATA_DIR/$IMP_MULTI/nfs-dir"
	sudo install -o "$IMP_UID_NFS" -g "$IMP_GRP_GID" -m 0644 /dev/null \
		"$DATA_DIR/$IMP_MULTI/nfs-dir/inner"

	echo "==> --with-impersonation: $IMP_OWNED and $IMP_MULTI made," \
		"unplaced;  $IMP_MULTI shared through $IMP_GRP_NAME"
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
	else
		echo "note: S3TEST_CONF is not set;  set lc_debug_interval = 10" \
			"in your s3tests.conf" >&2
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
	else
		echo "note: S3TEST_CONF is not set;  set kms_keyid = testkey-1" \
			"and kms_keyid2 = testkey-2 in your s3tests.conf" >&2
	fi
fi

# --- status + what-next output ---

FEATURE_FLAG="fails_on_nsfs,rgw_admin"
if [[ "$STORE" == "posix" ]]; then
	FEATURE_FLAG="fails_on_posix"
fi
if $VAULT; then
	FEATURE_FLAG="${FEATURE_FLAG},has_vault"
fi

echo ""
echo "==> radosgw up on port $RGW_PORT ($STORE, data on $DATA_DIR)"
if [[ -n "$S3CONF" ]]; then
	echo "    s3tests.conf: $S3CONF"
else
	echo "    s3tests.conf: S3TEST_CONF is not set;  it names your copy of the suite's s3tests.conf.SAMPLE"
fi
if $LIFECYCLE; then
	echo "    Lifecycle: rgw_lc_debug_interval=10"
fi
if $NON_MD5_ETAG; then
	echo "    ETag: rgw_non_md5_etag=true (mtime-ino, not a digest)"
fi
if $IMPERSONATE; then
	echo "    Impersonation: on;  bucket $IMP_BUCKET has open.txt (0644)" \
		"and grouped.txt (0040 root:$IMP_GRP_NAME, gid $IMP_GRP_GID)"
	echo "    Fixture ids: $IMP_USER_A=$IMP_UID_A in $IMP_GRP_NAME," \
		"$IMP_USER_B=$IMP_UID_B not"
	echo "    Unplaced: $DATA_DIR/$IMP_OWNED ($IMP_UID_A:$IMP_UID_A," \
		"0770/0660);  $DATA_DIR/$IMP_MULTI ($IMP_UID_A:$IMP_GRP_NAME," \
		"2770/0660, plus nfs-written and nfs-dir/ owned by" \
		"$IMP_UID_NFS:$IMP_GRP_NAME, 0644/2755)"
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
echo "To run tests, from your s3-tests-rs checkout's src/test/rgw/s3-tests-rs:"
echo "  S3TEST_CONF=${S3CONF:-/path/to/s3tests.conf} \\"
echo "    cargo nextest run -P baseline --features $FEATURE_FLAG"
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
