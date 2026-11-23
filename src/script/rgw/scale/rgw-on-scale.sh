#!/bin/bash
#
# rgw-on-scale.sh - deploy RGW as the S3 endpoint for IBM Storage Scale.
#
# Runs rgw-standalone against the nsfs (POSIX namespace) backend.
# No Ceph cluster, no mon, no OSD, no RADOS --
# the Scale filesystem is the only storage.
#
# Instances are systemd template units, so each one starts and stops
# independently:
#
#   systemctl start  rgw-on-scale@s1      # one endpoint
#   systemctl start  rgw-on-scale@s2      # two endpoints, same port
#   systemctl stop   rgw-on-scale@s1      # back to one
#   systemctl status 'rgw-on-scale@*'     # all of them
#   systemctl stop   rgw-on-scale.target  # all of them at once
#
# All instances share one TCP port via SO_REUSEPORT; the kernel load
# balances across them.  That matches the NooBaa multi-endpoint model
# CES already expects and needs no front-end proxy.
#
# Usage:
#   rgw-on-scale.sh install [OPTIONS]
#   rgw-on-scale.sh create user [OPTIONS]
#   rgw-on-scale.sh start|stop|restart [INSTANCE...]
#   rgw-on-scale.sh status
#   rgw-on-scale.sh uninstall [--purge]
#
# Run `rgw-on-scale.sh --help` for the full option list.

set -Eeuo pipefail

# `set -e` aborts silently on any unhandled non-zero status, so report
# where it happened.  -E propagates this trap into functions and subshells.
on_err() {
	local rc=$? line="${1:-?}"
	printf '\nerror: %s aborted at line %s (exit %d)\n' \
		"$(basename "${BASH_SOURCE[0]}")" "$line" "$rc" >&2
	exit "$rc"
}
trap 'on_err $LINENO' ERR

# ---------------------------------------------------------------- defaults

CONF_FILE=/etc/ceph/rgw-on-scale.conf
STATE_DIR=/var/lib/ceph/rgw-on-scale
LOG_DIR=/var/log/ceph
UNIT_DIR=/etc/systemd/system
SYSCONFIG_DIR=/etc/sysconfig

NSFS_ROOT=""
SHARED_ROOT=/var/lib/ceph/rgw-on-scale/shared
ENDPOINT=0.0.0.0:8000
INSTANCES=(s1 s2)
LC_INSTANCE=""
BUILD_DIR=""
BIN_DIR=/usr/bin
GPFS=false
DRY_RUN=false
PURGE=false
EXTRA_CONF=()

# Default create user identity.  Override with --access-key / --secret-key.
USER_UID=testid
USER_DISPLAY_NAME="M. Tester"
ACCESS_KEY="0555b35654ad1656d804"
SECRET_KEY="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# src/script/rgw/scale -> repo root
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

# What 'install' was given, so later subcommands do not need it repeated.
STATE_FILE="${RGW_ON_SCALE_STATE_FILE:-/etc/ceph/rgw-on-scale.env}"

# ----------------------------------------------------------------- helpers

say()  { printf '==> %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

run() {
	if $DRY_RUN; then
		printf '  [dry-run] %s\n' "$*"
	else
		"$@"
	fi
}

need_root() {
	$DRY_RUN && return 0
	[[ $EUID -eq 0 ]] || die "must run as root (systemd units and /etc/ceph)"
}

# Random key of $1 characters drawn from charset $2.
gen_key() {
	die "not implemented correctly yet!"
}

# rgw-standalone-admin against the deployed config, with its own LMDB
# root so it never shares one with a running instance.
admin() {
	"$BIN_DIR/rgw-standalone-admin" \
		-c "$CONF_FILE" \
		--rgw_nsfs_database_root="$STATE_DIR/admin" \
		"$@"
}

# Pull one field out of the admin tool's JSON.  Prefers python3
# (always present in a ceph build environment) and falls back to sed.
extract_key() {
	local field="$1" input
	input="$(cat)"
	if command -v python3 >/dev/null 2>&1; then
		printf '%s' "$input" | python3 -I -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
keys = d.get('keys') or []
if keys:
    print(keys[0].get('$field', ''))
" 2>/dev/null
	else
		printf '%s' "$input" \
			| grep -o "\"$field\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" \
			| sed -n '1s/.*:[[:space:]]*"//; 1s/"$//; 1p' || true
	fi
}

usage() {
	sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//; $d'
	cat <<'EOF'

Commands:
  install      preflight, create directories, write config, install units
  create user    create S3 account/user via rgw-standalone-admin
  credentials  print the current access/secret key for a user
  start        start instances (default: the ones from --instances)
  stop         stop instances (default: all running)
  restart      restart instances
  status       show every instance plus an endpoint probe
  uninstall    stop and remove units and config (--purge also drops state)

install options:
  --nsfs-root DIR       Scale filesystem directory that backs S3 buckets
                        (required; buckets are directories under it)
  --shared-root DIR     shared config/userdb location, i.e. the CES shared
                        root (default: /var/lib/ceph/rgw-on-scale/shared)
  --endpoint IP:PORT    listen address for every instance
                        (default: 0.0.0.0:8000)
  --instances "s1 s2"   instance names to enable (default: "s1 s2")
  --lc-instance NAME    instance that runs the lifecycle processor
                        (default: first of --instances; see NOTES)
  --build-dir DIR       run binaries from a ceph build tree instead of
                        /usr/bin, and render units from the source tree
  --gpfs                enable the GPFS-native fs strategy options
  -o key=value          config rgw options setting (repeatable)
  --dry-run             print what would happen, change nothing

create user options:
  --uid NAME            S3 user id (default: scale-s3)
  --access-key KEY      access key (default: generated)
  --secret-key KEY      secret key (default: generated)

NOTES
  Lifecycle runs on exactly one instance.  The LC tables live in the
  shared SQLite userdb and each instance would otherwise start its own RGWLC processor against it.
  Important: If the designated instance is stopped, lifecycle does not run until it comes back.
EOF
}

# ----------------------------------------------------------- install state

# Loaded before parse_args so explicit command-line flags still win.
load_state() {
	[[ -r "$STATE_FILE" ]] || return 0
	# shellcheck disable=SC1090
	source "$STATE_FILE"
	[[ -n "${SAVED_NSFS_ROOT:-}"   ]] && NSFS_ROOT="$SAVED_NSFS_ROOT"
	[[ -n "${SAVED_SHARED_ROOT:-}" ]] && SHARED_ROOT="$SAVED_SHARED_ROOT"
	[[ -n "${SAVED_ENDPOINT:-}"    ]] && ENDPOINT="$SAVED_ENDPOINT"
	[[ -n "${SAVED_BIN_DIR:-}"     ]] && BIN_DIR="$SAVED_BIN_DIR"
	[[ -n "${SAVED_LC_INSTANCE:-}" ]] && LC_INSTANCE="$SAVED_LC_INSTANCE"
	[[ -n "${SAVED_INSTANCES:-}"   ]] && read -r -a INSTANCES <<< "$SAVED_INSTANCES"
	return 0
}

save_state() {
	say "recording install settings in $STATE_FILE"
	if $DRY_RUN; then
		printf '  [dry-run] would write %s\n' "$STATE_FILE"
		return
	fi
	install -d -m 0755 "$(dirname "$STATE_FILE")"
	cat > "$STATE_FILE" <<-EOF
	# Written by rgw-on-scale.sh install.  Lets create user/start/stop/status
	# run without repeating the install options.  Command-line flags
	# override anything set here.
	SAVED_NSFS_ROOT='$NSFS_ROOT'
	SAVED_SHARED_ROOT='$SHARED_ROOT'
	SAVED_ENDPOINT='$ENDPOINT'
	SAVED_BIN_DIR='$BIN_DIR'
	SAVED_INSTANCES='${INSTANCES[*]}'
	SAVED_LC_INSTANCE='$LC_INSTANCE'
	EOF
	chmod 0644 "$STATE_FILE"
}

# ------------------------------------------------------------ arg handling

parse_args() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
			--nsfs-root)    NSFS_ROOT="$2"; shift 2 ;;
			--shared-root)  SHARED_ROOT="$2"; shift 2 ;;
			--endpoint)     ENDPOINT="$2"; shift 2 ;;
			--instances)    read -r -a INSTANCES <<< "$2"; shift 2 ;;
			--lc-instance)  LC_INSTANCE="$2"; shift 2 ;;
			--build-dir)    BUILD_DIR="$2"; shift 2 ;;
			--gpfs)         GPFS=true; shift ;;
			--uid)          USER_UID="$2"; shift 2 ;;
			--access-key)   ACCESS_KEY="$2"; shift 2 ;;
			--secret-key)   SECRET_KEY="$2"; shift 2 ;;
			-o)             EXTRA_CONF+=("$2"); shift 2 ;;
			--dry-run)      DRY_RUN=true; shift ;;
			--purge)        PURGE=true; shift ;;
			-h|--help)      usage; exit 0 ;;
			--)             shift; POSITIONAL+=("$@"); break ;;
			-*)             die "unknown option: $1" ;;
			*)              POSITIONAL+=("$1"); shift ;;
		esac
	done

	if [[ -n "$BUILD_DIR" ]]; then
		BUILD_DIR="$(cd "$BUILD_DIR" && pwd)"
		BIN_DIR="$BUILD_DIR/bin"
	fi
	[[ -n "$LC_INSTANCE" ]] || LC_INSTANCE="${INSTANCES[0]}"
}

# --------------------------------------------------------------- preflight

preflight() {
	need_root

	[[ -n "$NSFS_ROOT" ]] || die "--nsfs-root is required"
	[[ -d "$NSFS_ROOT" ]] || die "--nsfs-root $NSFS_ROOT does not exist"

	for b in rgw-standalone rgw-standalone-admin; do
		[[ -x "$BIN_DIR/$b" ]] || die "$BIN_DIR/$b not found or not executable"
	done

	local port="${ENDPOINT##*:}"
	[[ "$port" =~ ^[0-9]+$ ]] || die "--endpoint must be IP:PORT, got '$ENDPOINT'"

	# Every instance binds the same port with SO_REUSEPORT, so an existing
	# listener is only a problem if it is not one of ours.
	if command -v ss >/dev/null 2>&1; then
		local inuse
		inuse="$(ss -ltnp "sport = :$port" 2>/dev/null | tail -n +2 || true)"
		if [[ -n "$inuse" ]] && [[ "$inuse" != *rgw-standalone* ]]; then
			warn "port $port is already in use by a non-RGW process"
		fi
	fi

	if $GPFS && [[ ! -e /usr/lpp/mmfs/lib/libgpfs.so ]]; then
		warn "--gpfs given but /usr/lpp/mmfs/lib/libgpfs.so not found;" \
		     "the driver will fall back to the portable POSIX strategy"
	fi

	local lc_found=false
	for i in "${INSTANCES[@]}"; do
		[[ "$i" == "$LC_INSTANCE" ]] && lc_found=true
	done
	$lc_found || warn "--lc-instance $LC_INSTANCE is not in --instances;" \
	                  "lifecycle will not run unless you start it manually"
}

# --------------------------------------------------------- config rendering

write_conf() {
	say "writing $CONF_FILE"

	local tmp
	tmp="$(mktemp)"

	{
		cat <<-EOF
		# RGW on IBM Storage Scale -- generated by rgw-on-scale.sh
		# $(date -u +%Y-%m-%dT%H:%M:%SZ)
		#
		# Per-instance settings (LMDB listing cache root, log file, admin
		# socket, luarocks dir) are passed on the command line by
		# rgw-on-scale@.service and derived from the instance name.  Do
		# not set rgw_nsfs_database_root here: instances must not share
		# an LMDB root.

		[client]
		        rgw_backend_store = nsfs
		        rgw_config_store = dbstore
		        dbstore_config_uri = file://$SHARED_ROOT/config.db

		        rgw_nsfs_base_path = $NSFS_ROOT
		        rgw_nsfs_userdb_dir = $SHARED_ROOT/userdb

		        rgw_frontends = beast endpoint=$ENDPOINT so_reuseport=1

		        # With more than one instance per node, each process has its
		        # own listing cache and cannot see the other's writes.
		        # inotify closes that gap.
		        rgw_nsfs_inotify = true

		        # Lifecycle is pinned to a single instance; see below.
		        rgw_enable_lc_threads = false
		EOF

		if $GPFS; then
			cat <<-EOF

			        # GPFS-native filesystem strategy
			        rgw_nsfs_gpfs_lib_path = /usr/lpp/mmfs/lib/libgpfs.so
			        rgw_nsfs_gpfs_batch_xattrs = true
			        # LWE gives cluster-wide version locking across nodes;
			        # OFD locks only serialize within one kernel.
			        rgw_nsfs_gpfs_lwe_locking = true
			EOF
		fi

		if [[ ${#EXTRA_CONF[@]} -gt 0 ]]; then
			printf '\n'
			for kv in "${EXTRA_CONF[@]}"; do
				printf '        %s = %s\n' "${kv%%=*}" "${kv#*=}"
			done
		fi

		cat <<-EOF

		# Lifecycle processor.  The LC tables live in the shared SQLite
		# userdb, so exactly one instance may run RGWLC.
		[client.rgw.$LC_INSTANCE]
		        rgw_enable_lc_threads = true
		EOF
	} > "$tmp"

	if $DRY_RUN; then
		printf '  [dry-run] would write %s:\n' "$CONF_FILE"
		sed 's/^/    | /' "$tmp"
		rm -f "$tmp"
	else
		install -d -m 0755 "$(dirname "$CONF_FILE")"
		install -m 0644 "$tmp" "$CONF_FILE"
		rm -f "$tmp"
	fi
}

# ------------------------------------------------------------ unit handling

install_units() {
	# RPM installs the units already.  From a build tree we render them
	# ourselves so developers can deploy without packaging first.
	if [[ -z "$BUILD_DIR" ]] && [[ -f /usr/lib/systemd/system/rgw-on-scale@.service ]]; then
		say "using packaged units from /usr/lib/systemd/system"
		return
	fi

	local src="$REPO_ROOT/systemd/rgw-on-scale@.service.in"
	local tgt="$REPO_ROOT/systemd/rgw-on-scale.target"
	[[ -f "$src" ]] || die "$src not found (run from a ceph source tree, or install the RPM)"

	say "installing units into $UNIT_DIR (binaries: $BIN_DIR)"

	local prefix="${BIN_DIR%/bin}"
	local tmp
	tmp="$(mktemp)"
	sed -e "s|@CMAKE_INSTALL_PREFIX@|$prefix|g" \
	    -e "s|@CEPH_SYSTEMD_ENV_DIR@|$SYSCONFIG_DIR|g" \
	    "$src" > "$tmp"

	if $DRY_RUN; then
		printf '  [dry-run] would install %s/rgw-on-scale@.service\n' "$UNIT_DIR"
		printf '  [dry-run] would install %s/rgw-on-scale.target\n' "$UNIT_DIR"
		rm -f "$tmp"
		return
	fi

	install -m 0644 "$tmp" "$UNIT_DIR/rgw-on-scale@.service"
	install -m 0644 "$tgt" "$UNIT_DIR/rgw-on-scale.target"
	rm -f "$tmp"
	systemctl daemon-reload
}

# ---------------------------------------------------------------- commands

create_user_rgw_on_scale() {
	need_root
	[[ -f "$CONF_FILE" ]] || die "$CONF_FILE not found -- run 'install' first"
	[[ -x "$BIN_DIR/rgw-standalone-admin" ]] || die "$BIN_DIR/rgw-standalone-admin not found"

	[[ -n "$ACCESS_KEY" ]] || ACCESS_KEY="$(gen_key 20 'A-Z0-9')"
	[[ -n "$SECRET_KEY" ]] || SECRET_KEY="$(gen_key 40 'A-Za-z0-9')"

	if [[ -n "$(systemctl list-units --no-legend --state=active \
	            'rgw-on-scale@*.service' 2>/dev/null)" ]]; then
		warn "instances are running; the SQLite userdb is opened without WAL" \
		     "or a busy timeout, so a concurrent write may fail with SQLITE_BUSY."
		warn "if 'user create' fails, stop the instances and retry."
	fi

	say "creating S3 user $USER_UID"

	# Give the admin tool its own LMDB root.  Sharing one with a running
	# instance would be destructive: the BucketCache constructor purges
	# the directory it is handed.
	run install -d -m 0750 "$STATE_DIR/admin"

	if $DRY_RUN; then
		printf '  [dry-run] %s -c %s user create --uid %s ...\n' \
			"$BIN_DIR/rgw-standalone-admin" "$CONF_FILE" "$USER_UID"
		return
	fi

	local out err rc=0
	err="$(mktemp)"

	# Do not let a non-zero exit trip `set -e` before we can report it.
	out="$(admin user create \
		--uid "$USER_UID" \
		--display-name "$USER_DISPLAY_NAME" \
		--access-key "$ACCESS_KEY" \
		--secret "$SECRET_KEY" 2>"$err")" || rc=$?

	if [[ $rc -ne 0 ]]; then
		# The usual cause is a user left over from an earlier run.  Fall
		# back to reading it, so create user is idempotent and still hands
		# back usable credentials.
		local info irc=0
		info="$(admin user info --uid "$USER_UID" 2>/dev/null)" || irc=$?
		if [[ $irc -eq 0 ]] && printf '%s' "$info" | grep -q '"access_key"'; then
			warn "user $USER_UID already exists; showing its current credentials"
			out="$info"
		else
			printf 'error: rgw-standalone-admin user create failed (exit %d)\n' "$rc" >&2
			printf -- '--- last 25 lines of rgw-standalone-admin output ---\n' >&2
			tail -n 25 "$err" >&2
			rm -f "$err"
			exit 1
		fi
	fi
	rm -f "$err"

	# The tool can emit sqlite chatter on stdout ahead of the JSON.
	local json
	json="$(printf '%s\n' "$out" | sed -n '/^{/,$p')"

	local akey skey
	akey="$(printf '%s' "$json" | extract_key access_key)"
	skey="$(printf '%s' "$json" | extract_key secret_key)"

	if [[ -z "$akey" || -z "$skey" ]]; then
		warn "could not parse credentials out of rgw-standalone-admin output"
		printf '%s\n' "$out"
		printf '\nRecover them with:\n  %s -c %s user info --uid %s\n\n' \
			"$BIN_DIR/rgw-standalone-admin" "$CONF_FILE" "$USER_UID"
		exit 1
	fi

	cat <<EOF

S3 credentials for $USER_UID:
  endpoint    http://$ENDPOINT
  access key  $akey
  secret key  $skey

Try them with:
  export AWS_ACCESS_KEY_ID=$akey
  export AWS_SECRET_ACCESS_KEY=$skey
  export AWS_DEFAULT_REGION=us-east-1
  aws --endpoint-url http://${ENDPOINT/0.0.0.0/127.0.0.1} s3 ls

Show them again at any time with:
  $0 credentials

EOF
}

# Start every install with an empty log, same way vstart.sh wipes
# build/out before bringing a cluster up: to be able to read the log clean
# without mixes from previous runs.
#
# $LOG_DIR defaults to /var/log/ceph, which is shared with unrelated ceph
# daemons, so only this tool's own files are touched.
reset_logs() {
	local f live=()

	for f in "$LOG_DIR"/rgw-on-scale.*.log; do
		[[ -f "$f" ]] && live+=("$f")
	done

	[[ ${#live[@]} -gt 0 ]] || return 0

	say "resetting logs in $LOG_DIR"
	[[ ${#live[@]} -gt 0 ]] && run truncate -s 0 "${live[@]}"
	return 0
}

install_rgw_on_scale() {
	preflight

	say "creating directories"
	run install -d -m 0750 "$SHARED_ROOT" "$SHARED_ROOT/userdb"
	run install -d -m 0750 "$STATE_DIR"
	run install -d -m 0755 "$LOG_DIR"
	for i in "${INSTANCES[@]}"; do
		run install -d -m 0750 "$STATE_DIR/$i"
	done

	reset_logs
	write_conf
	install_units
	save_state
	create_user_rgw_on_scale

	# A failed unit from an earlier attempt blocks a restart with
	# "Start request repeated too quickly".
	run systemctl reset-failed 'rgw-on-scale@*' 2>/dev/null || true

	say "enabling instances: ${INSTANCES[*]}"
	run systemctl enable rgw-on-scale.target
	for i in "${INSTANCES[@]}"; do
		run systemctl enable "rgw-on-scale@$i"
	done

	cat <<EOF

Installed.  Next:

  systemctl start rgw-on-scale@${INSTANCES[0]}
  systemctl status 'rgw-on-scale@*'   # or: $0 status

Instances can be started and stopped individually from here on:

  systemctl start rgw-on-scale@${INSTANCES[0]}
  systemctl start rgw-on-scale@${INSTANCES[1]:-s2}
  systemctl start  rgw-on-scale.target
  systemctl stop  rgw-on-scale@${INSTANCES[0]}
  systemctl stop  rgw-on-scale.target

EOF
}

credentials_rgw_on_scale() {
	need_root
	[[ -f "$CONF_FILE" ]] || die "$CONF_FILE not found -- run 'install' first"
	[[ -x "$BIN_DIR/rgw-standalone-admin" ]] || die "$BIN_DIR/rgw-standalone-admin not found"

	local json
	json="$(admin user info --uid "$USER_UID" 2>/dev/null | sed -n '/^{/,$p')" \
		|| die "no such user: $USER_UID (run 'create user' first)"

	local akey skey
	akey="$(printf '%s' "$json" | extract_key access_key)"
	skey="$(printf '%s' "$json" | extract_key secret_key)"
	[[ -n "$akey" ]] || die "no such user: $USER_UID (run 'create user' first)"

	cat <<EOF
  endpoint    http://$ENDPOINT
  access key  $akey
  secret key  $skey
EOF
}

units_for() {
	if [[ ${#POSITIONAL[@]} -gt 1 ]]; then
		local i
		for i in "${POSITIONAL[@]:1}"; do printf 'rgw-on-scale@%s\n' "$i"; done
	else
		for i in "${INSTANCES[@]}"; do printf 'rgw-on-scale@%s\n' "$i"; done
	fi
}

start_rgw_on_scale()   { need_root; mapfile -t u < <(units_for); run systemctl start "${u[@]}"; rgw_on_scale_status; }
restart_rgw_on_scale() { need_root; mapfile -t u < <(units_for); run systemctl restart "${u[@]}"; rgw_on_scale_status; }

stop_rgw_on_scale() {
	need_root
	if [[ ${#POSITIONAL[@]} -gt 1 ]]; then
		mapfile -t u < <(units_for)
		run systemctl stop "${u[@]}"
	else
		run systemctl stop 'rgw-on-scale@*.service'
	fi
	rgw_on_scale_status
}

rgw_on_scale_status() {
	local active=0 line unit state

	printf '\n%-24s %-10s %-8s %s\n' INSTANCE STATE PID "SINCE"
	printf '%-24s %-10s %-8s %s\n' "------------------------" "----------" "--------" "-----"

	while read -r unit _; do
		[[ -n "$unit" ]] || continue
		state="$(systemctl show -p ActiveState --value "$unit" 2>/dev/null)"
		local pid since
		pid="$(systemctl show -p MainPID --value "$unit" 2>/dev/null)"
		since="$(systemctl show -p ActiveEnterTimestamp --value "$unit" 2>/dev/null)"
		[[ "$state" == active ]] && active=$((active + 1))
		printf '%-24s %-10s %-8s %s\n' "${unit%.service}" "$state" "$pid" "${since:--}"
	done < <(systemctl list-units --no-legend --all --plain \
	           'rgw-on-scale@*.service' 2>/dev/null | awk '{print $1}')

	printf '\n%d instance(s) running\n' "$active"

	# An endpoint probe tells us the S3 stack is up, not just the process.
	if [[ -f "$CONF_FILE" ]]; then
		local ep code
		ep="$(awk -F'endpoint=' '/rgw_frontends/ {split($2,a," "); print a[1]}' "$CONF_FILE")"
		ep="${ep/0.0.0.0/127.0.0.1}"
		if [[ -n "$ep" ]] && command -v curl >/dev/null 2>&1; then
			code="$(curl -s -o /dev/null -m 5 -w '%{http_code}' "http://$ep/" 2>/dev/null || echo 000)"
			if [[ "$code" == 000 ]]; then
				printf 'endpoint http://%s  NOT RESPONDING\n' "$ep"
			else
				# 403 AccessDenied to an anonymous GET / means the S3
				# request path is fully wired up.
				printf 'endpoint http://%s  responding (HTTP %s)\n' "$ep" "$code"
			fi
		fi
	fi
	printf '\n'
}

uninstall_on_scale_status() {
	need_root

	say "stopping all instances"
	run systemctl stop 'rgw-on-scale@*.service' || true
	run systemctl disable rgw-on-scale.target || true
	for i in "${INSTANCES[@]}"; do
		run systemctl disable "rgw-on-scale@$i" || true
	done

	if [[ -f "$UNIT_DIR/rgw-on-scale@.service" ]]; then
		say "removing units from $UNIT_DIR"
		run rm -f "$UNIT_DIR/rgw-on-scale@.service" "$UNIT_DIR/rgw-on-scale.target"
		run systemctl daemon-reload
	fi

	run rm -f "$CONF_FILE" "$STATE_FILE"

	if $PURGE; then
		# Config and caches only.  The nsfs data root holds customer
		# buckets and is never touched.
		say "--purge: removing $STATE_DIR and $SHARED_ROOT"
		run rm -rf "$STATE_DIR" "$SHARED_ROOT"
	else
		say "leaving state in $STATE_DIR and $SHARED_ROOT (use --purge to remove)"
	fi
}

# -------------------------------------------------------------------- main

POSITIONAL=()
load_state
parse_args "$@"

[[ ${#POSITIONAL[@]} -gt 0 ]] || { usage; exit 1; }

case "${POSITIONAL[0]}" in
	create)
	  [[ ${#POSITIONAL[@]} -gt 1 ]] || { usage; exit 1; }
	  case "${POSITIONAL[1]}" in
          user) create_user_rgw_on_scale ;;
          *)    die "unknown subcommand for create: ${POSITIONAL[1]} (try --help)" ;;
    esac ;;
	install)   install_rgw_on_scale ;;
	credentials) credentials_rgw_on_scale ;;
	start)     start_rgw_on_scale ;;
	stop)      stop_rgw_on_scale ;;
	restart)   restart_rgw_on_scale ;;
	status)    rgw_on_scale_status ;;
	uninstall) uninstall_on_scale_status ;;
	*)         die "unknown command: ${POSITIONAL[0]} (try --help)" ;;
esac
