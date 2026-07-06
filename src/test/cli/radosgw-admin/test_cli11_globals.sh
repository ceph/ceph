#!/bin/bash
# CLI11 migration tests for cross-cutting global handling:
#   1. ceph global flags stripped by rgw_global_init before CLI11 parses
#   2. the --cli11-help "Ceph options" footer (every command, aligned)
#
# Usage:
#   ./test_cli11_globals.sh
#   RGW_ADMIN=/path/to/radosgw-admin ./test_cli11_globals.sh
#   CEPH_CONF=/path/to/ceph.conf ./test_cli11_globals.sh
#
# Run from the build directory:
#   cd /path/to/ceph/build && bash /path/to/test_cli11_globals.sh

RGW_ADMIN="${RGW_ADMIN:-./bin/radosgw-admin}"
export CEPH_CONF="${CEPH_CONF:-./ceph.conf}"
# Route dout/derr log lines off stderr so async cluster logs can't interleave
# mid-line with the cerr messages we grep. Consumed by rgw_global_init.
export CEPH_ARGS="--log-to-stderr=false${CEPH_ARGS:+ ${CEPH_ARGS}}"
PASS=0
FAIL=0
SKIP=0

# Filter out noisy ceph log lines and config-not-found lines
filter() {
  grep -v "^[0-9]\{4\}-" | \
  grep -v "^did not load config" | \
  grep -v "^unable to get monitor" | \
  grep -v "^failed to fetch mon config"
}

# --no-mon-config skips monitor connection so check()/help checks run without a cluster.
_run() { "$RGW_ADMIN" --no-mon-config "$@"; }

cluster_running() { pgrep -x radosgw > /dev/null 2>&1; }

check() {
  local desc="$1" expected_exit="$2" expected_msg="$3"
  shift 3
  local tmpfile; tmpfile=$(mktemp)
  _run "$@" >"$tmpfile" 2>&1
  local exit_code=$?
  local output; output=$(filter <"$tmpfile")
  rm -f "$tmpfile"

  local ok=1
  if [ "$exit_code" != "$expected_exit" ]; then
    echo "FAIL [$desc]: expected exit $expected_exit, got $exit_code"
    echo "     output: $output"
    ok=0
  fi
  if [ -n "$expected_msg" ] && ! echo "$output" | grep -qF -- "$expected_msg"; then
    echo "FAIL [$desc]: expected message not found: $expected_msg"
    echo "     output: $output"
    ok=0
  fi
  [ "$ok" = "1" ] && { echo "PASS [$desc]"; PASS=$((PASS+1)); } || FAIL=$((FAIL+1))
}

# check_cluster "desc" expected_exit "expected_msg_or_empty" -- command args
check_cluster() {
  local desc="$1" expected_exit="$2" expected_msg="$3"
  shift 3
  shift  # skip --

  if ! cluster_running; then
    echo "SKIP [$desc]: no cluster running"
    SKIP=$((SKIP+1))
    return
  fi

  local tmpfile; tmpfile=$(mktemp)
  "$RGW_ADMIN" "$@" >"$tmpfile" 2>&1
  local exit_code=$?
  local output; output=$(filter <"$tmpfile")
  rm -f "$tmpfile"

  local ok=1
  if [ "$exit_code" != "$expected_exit" ]; then
    echo "FAIL [$desc]: expected exit $expected_exit, got $exit_code"
    echo "     output: $output"
    ok=0
  fi
  if [ -n "$expected_msg" ] && ! echo "$output" | grep -qF -- "$expected_msg"; then
    echo "FAIL [$desc]: expected message not found: $expected_msg"
    echo "     output: $output"
    ok=0
  fi
  [ "$ok" = "1" ] && { echo "PASS [$desc]"; PASS=$((PASS+1)); } || FAIL=$((FAIL+1))
}

# check_help_content "desc" "expected_substring" -- command args
# Asserts exit 0 and that the (verbatim) substring appears in help output.
check_help_content() {
  local desc="$1" expected_content="$2"; shift 2
  local output; output=$(_run "$@" 2>&1)
  local exit_code=$?
  if [ "$exit_code" != "0" ]; then
    echo "FAIL [$desc]: expected exit 0, got $exit_code"
    FAIL=$((FAIL+1))
    return
  fi
  if ! echo "$output" | grep -qF -- "$expected_content"; then
    echo "FAIL [$desc]: expected '$expected_content' in help output"
    echo "     output: $(echo "$output" | head -3)"
    FAIL=$((FAIL+1))
  else
    echo "PASS [$desc]"
    PASS=$((PASS+1))
  fi
}

# ============================================================
# ceph global flags are consumed by rgw_global_init (ceph_argparse early_args +
# md_config) BEFORE CLI11 parses, so CLI11 never sees them and cannot reject them
# as "unexpected argument" / "invalid flag". Regression: space-form values (e.g.
# --rgw-zone foo) used to strand as a stray positional and get rejected. One
# global per parsing tier, in space and = form, plus no-value and pre-command.
# object shard is a local computation (exit 0, prints "shard": 10), so a global
# that leaked to CLI11 would instead surface as exit != 0 + "unexpected argument".
# ============================================================
echo ""
echo "=== ceph globals stripped before CLI11 (no cluster) ==="
_SHARD='bucket object shard --object foo --num-shards 11'
check "global tier1 --cluster (space) stripped"            0 '"shard": 10' $_SHARD --cluster ceph
check "global tier1 --cluster=ceph (= form) stripped"      0 '"shard": 10' $_SHARD --cluster=ceph
check "global tier1 --no-config-file (no value) stripped"  0 '"shard": 10' $_SHARD --no-config-file
check "global tier2 -d (no value) stripped"                0 '"shard": 10' $_SHARD -d
check "global tier3 --rgw-zone (space) stripped [regression]" 0 '"shard": 10' $_SHARD --rgw-zone default
check "global tier3 --rgw-zone=default (= form) stripped"   0 '"shard": 10' $_SHARD --rgw-zone=default
check "global tier3 --debug-rgw (space) stripped"          0 '"shard": 10' $_SHARD --debug-rgw 5
check "global tier3 --debug-rgw=5 (= form) stripped"       0 '"shard": 10' $_SHARD --debug-rgw=5
check "global --rgw-zone default before command stripped"  0 '"shard": 10' --rgw-zone default $_SHARD

echo ""
echo "=== ceph globals stripped before CLI11 (cluster) ==="
check_cluster "global --rgw-zone default (space) on bucket list"        0 "" -- bucket list --rgw-zone default
check_cluster "global --rgw-zone=default (= form) on bucket list"       0 "" -- bucket list --rgw-zone=default
check_cluster "global --debug-rgw 5 (space) on bucket list"             0 "" -- bucket list --debug-rgw 5
check_cluster "global --rgw-zone default before command on bucket list" 0 "" -- --rgw-zone default bucket list

# ============================================================
# --cli11-help footer: ceph globals can't be real CLI11 options (stripped before
# parse), so they are documented in the footer. Three properties:
#   everywhere - the "Ceph options" footer is set before the subcommands are
#                created, so footer_ is copied to every child and each command's
#                help is self-contained (root and subcommands all show it).
#   own globals - --tenant/--uid ARE real root options and DO inherit into every
#                subcommand's help.
#   aligned    - footer paragraph formatting is disabled, so the column padding
#                is emitted verbatim (multi-space gaps survive).
# ============================================================
echo ""
echo "=== --cli11-help footer at root (no cluster) ==="
check_help_content "root footer heading"      "Ceph options:"       --cli11-help
check_help_content "root footer --conf"        "--conf/-c FILE"       --cli11-help
check_help_content "root footer --cluster"     "--cluster NAME"       --cli11-help
check_help_content "root footer --id/--user"   "--id/--user ID"       --cli11-help
check_help_content "root footer --name"        "--name/-n TYPE.ID"    --cli11-help
check_help_content "root footer --no-config-file" "--no-config-file"  --cli11-help
check_help_content "root footer --setuser"     "--setuser USER"       --cli11-help
check_help_content "root footer --setgroup"    "--setgroup GROUP"     --cli11-help
check_help_content "root footer --version"     "--version/-v"         --cli11-help
check_help_content "root footer --show_args"   "--show_args"          --cli11-help
check_help_content "root footer config placeholder" "--<config>=<value>" --cli11-help
# alignment: multi-space gap between flag column and description survives verbatim
check_help_content "root footer aligned (padding kept)" "FILE       read configuration" --cli11-help
# radosgw-admin's own globals render as real options at root
check_help_content "root own-global --tenant" "--tenant"             --cli11-help
check_help_content "root own-global --uid"     "--uid"                --cli11-help

echo ""
echo "=== --cli11-help footer on bucket subcommands (no cluster) ==="
# subcommand help is self-contained: the ceph footer renders there too ...
check_help_content "bucket link: footer heading"    "Ceph options:"  bucket link --cli11-help
check_help_content "bucket link: footer --conf"     "--conf/-c"      bucket link --cli11-help
check_help_content "bucket link: footer --setuser"  "--setuser"      bucket link --cli11-help
check_help_content "bucket link: footer --setgroup" "--setgroup"     bucket link --cli11-help
# alignment survives in subcommand help too (formatter is shared with children)
check_help_content "bucket link: footer aligned (padding kept)" "FILE       read configuration" bucket link --cli11-help
# ... and own globals inherit into subcommand help as real options
check_help_content "bucket link: own-global --tenant" "--tenant"    bucket link --cli11-help
check_help_content "bucket link: own-global --uid"     "--uid"       bucket link --cli11-help

echo ""
echo "=== --cli11-help footer on script subcommands (no cluster) ==="
check_help_content "script get: footer heading"    "Ceph options:"  script get --cli11-help
check_help_content "script get: footer --conf"     "--conf/-c"      script get --cli11-help
check_help_content "script get: footer --setuser"  "--setuser"      script get --cli11-help
check_help_content "script get: footer --setgroup" "--setgroup"     script get --cli11-help
check_help_content "script put: footer heading"    "Ceph options:"  script put --cli11-help
check_help_content "script rm: footer heading"     "Ceph options:"  script rm --cli11-help
# own globals inherit into script subcommand help too
check_help_content "script get: own-global --tenant" "--tenant"     script get --cli11-help
check_help_content "script get: own-global --uid"     "--uid"        script get --cli11-help
check_help_content "script put: own-global --tenant" "--tenant"     script put --cli11-help

# ============================================================
echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$SKIP" -gt 0 ] && echo "(some tests require a running cluster)"
echo "========================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
