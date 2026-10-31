#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@ibm.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
# test_batch_mixed.sh — S3-level mirror of batch_mixed_test.
#
# Three phases of 100 iterations each:
#   Phase 1: short keys   (1-32 chars),  mixed data sizes  (0-8192 bytes)
#   Phase 2: long keys    (512-1024 chars), mixed data sizes (0-8192 bytes)
#   Phase 3: storage-only (376-1024 chars), data sizes 4097-8192 bytes, 5-10 entries
#
# Each iteration submits N objects concurrently via s5cmd, then GET-verifies
# each one.  After all iterations a final sweep re-verifies everything.
# Batch stats are delta-sampled via the admin socket before/after each phase.
# Cleanup: s3cmd rb --recursive + bucket-absent verification.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/kvrgw-common.sh"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
VERBOSE=0
for arg in "$@"; do
  [[ "$arg" == "--verbose" || "$arg" == "-v" ]] && VERBOSE=1
done

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Bypass nginx and talk directly to instance 0 for PUT/GET/DELETE, so that
# instance 0's get-batch-stats sees 100% of this test's traffic instead of
# only its share of nginx's round-robin load balancing across KVRGW_INSTANCES.
INSTANCE0_PORT="$(kvrgw_instance_http_port 0)"
ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:${INSTANCE0_PORT}}"
INSTANCE0_HOST="127.0.0.1:${INSTANCE0_PORT}"
ADMIN_SOCK="/tmp/kvrgw-admin-0.sock"
NUM_ITERATIONS=100

TMPDIR_ROOT="$(mktemp -d /tmp/test_batch_mixed.XXXXXX)"

# Buckets are appended here only after their create-bucket call succeeds, so
# cleanup_on_exit() only ever attempts to delete buckets known to exist.
CREATED_BUCKETS=()

# Guarantees S3 bucket/object cleanup and local tmpdir removal on every exit
# path (success or failure). Bash only honors one EXIT trap, so both local
# and S3 cleanup are folded into this single function. On the normal success
# path run_phase() already deletes its own bucket before exiting; the
# attempt here on an already-deleted bucket is a harmless no-op (best-effort
# safety net, errors suppressed).
cleanup_on_exit() {
  local b
  for b in "${CREATED_BUCKETS[@]:-}"; do
    [[ -n "$b" ]] && s3cmd -c "${S3CMD_CFG}" --host="${INSTANCE0_HOST}" --host-bucket="${INSTANCE0_HOST}" \
      rb --recursive "s3://${b}" >/dev/null 2>&1 || true
  done
  rm -rf "$TMPDIR_ROOT"
}
trap cleanup_on_exit EXIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
admin_cmd() {
  echo "$1" | socat -T 5 - UNIX-CONNECT:"$ADMIN_SOCK" 2>/dev/null
}

aws_s3api() {
  aws --endpoint-url="$ENDPOINT" s3api "$@"
}

# Create a bucket and track it in CREATED_BUCKETS so cleanup_on_exit() can
# guarantee removal even if the script fails later.
create_bucket_tracked() {
  local bucket="$1"
  if ! aws_s3api create-bucket --bucket "$bucket" >/dev/null 2>&1; then
    fail "create-bucket failed for bucket=$bucket"
  fi
  CREATED_BUCKETS+=("$bucket")
}

fail() {
  echo -e "${RED}FAIL: $1${NC}" >&2
  exit 1
}

check() {
  local label="$1" lhs="$2" rhs="$3"
  if [[ "$lhs" -eq "$rhs" ]]; then
    echo -e "${GREEN}  [OK]   ${label}  (${lhs} == ${rhs})${NC}"
  else
    echo -e "${RED}  [FAIL] ${label}  (${lhs} != ${rhs})${NC}"
    fail "BatchStats sanity check failed: ${label}  (${lhs} != ${rhs})"
  fi
}

# Generate a random lowercase alphanumeric string of exactly LEN chars.
# Returns empty string (no output) when LEN=0.
rand_chars() {
  local len="$1"
  [[ "$len" -le 0 ]] && return 0
  LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom 2>/dev/null | head -c "$len"
}

# Generate an object name: prefix i${ITER}e${ENTRY}- + random chars in [MIN,MAX].
make_obj_name() {
  local iter="$1" entry="$2" min_len="$3" max_len="$4"
  local prefix="i${iter}e${entry}-"
  local range=$(( max_len - min_len + 1 ))
  local total_len=$(( min_len + (RANDOM * RANDOM) % range ))
  local pfx_len=${#prefix}
  local rand_len=$(( total_len > pfx_len ? total_len - pfx_len : 0 ))
  printf '%s' "$prefix"
  [[ "$rand_len" -gt 0 ]] && rand_chars "$rand_len"
  printf '\n'
}

# Make a random 32-char lowercase bucket name starting with a letter.
make_bucket_name() {
  local first
  first=$(LC_ALL=C tr -dc 'a-z' < /dev/urandom 2>/dev/null | head -c 1)
  local rest
  rest=$(rand_chars 31)
  echo "${first}${rest}"
}

# Generate a data file of SIZE random bytes from /dev/urandom (0 = empty file).
make_data_file() {
  local path="$1" size="$2"
  if [[ "$size" -eq 0 ]]; then
    : > "$path"
  else
    dd if=/dev/urandom of="$path" bs="$size" count=1 2>/dev/null
  fi
}

# ---------------------------------------------------------------------------
# Stats snapshot / delta helpers
# ---------------------------------------------------------------------------
# Snapshot all batch stat counters into variables prefixed snap_*
_parse_field() {
  echo "$1" | grep -o "${2}=[0-9]*" | head -1 | cut -d= -f2 || echo 0
}

snapshot_stats() {
  local raw
  raw=$(admin_cmd "get-batch-stats")
  if [[ "$raw" != *"attempted_batch_commits="* ]]; then
    fail "get-batch-stats failed or returned unexpected response: '$raw'"
  fi
  snap_attempted_batch_commits=$(_parse_field "$raw" "attempted_batch_commits")
  snap_attempted_entries_batched=$(_parse_field "$raw" "attempted_entries_batched")
  snap_attempted_single_entry_batches=$(_parse_field "$raw" "attempted_single_entry_batches")
  snap_attempted_inline_only_batch_commits=$(_parse_field "$raw" "attempted_inline_only_batch_commits")
  snap_attempted_inline_only_batch_entries=$(_parse_field "$raw" "attempted_inline_only_batch_entries")
  snap_committed_batch_commits=$(_parse_field "$raw" "committed_batch_commits")
  snap_committed_total_entries=$(_parse_field "$raw" "committed_total_entries")
  snap_committed_single_entry_batches=$(_parse_field "$raw" "committed_single_entry_batches")
  snap_committed_inline_only_batch_commits=$(_parse_field "$raw" "committed_inline_only_batch_commits")
  snap_committed_inline_only_batch_entries=$(_parse_field "$raw" "committed_inline_only_batch_entries")
  snap_conflict_pushbacks=$(_parse_field "$raw" "conflict_pushbacks")
  snap_fallback_to_single_count=$(_parse_field "$raw" "fallback_to_single_count")
  snap_fallback_to_single_entries=$(_parse_field "$raw" "fallback_to_single_entries")
  snap_single_txn_puts=$(_parse_field "$raw" "single_txn_puts")
}

print_phase_stats() {
  local phase_label="$1" total_sent="$2"
  local raw
  raw=$(admin_cmd "get-batch-stats")
  if [[ "$raw" != *"attempted_batch_commits="* ]]; then
    fail "get-batch-stats failed or returned unexpected response: '$raw'"
  fi

  local cur_a_commits cur_a_entries cur_a_single
  local cur_a_iol_commits cur_a_iol_entries
  local cur_c_commits cur_c_entries cur_c_single
  local cur_c_iol_commits cur_c_iol_entries
  local cur_conflict cur_fallback cur_fallback_entries

  cur_a_commits=$(_parse_field "$raw" "attempted_batch_commits")
  cur_a_entries=$(_parse_field "$raw" "attempted_entries_batched")
  cur_a_single=$(_parse_field "$raw" "attempted_single_entry_batches")
  cur_a_iol_commits=$(_parse_field "$raw" "attempted_inline_only_batch_commits")
  cur_a_iol_entries=$(_parse_field "$raw" "attempted_inline_only_batch_entries")
  cur_c_commits=$(_parse_field "$raw" "committed_batch_commits")
  cur_c_entries=$(_parse_field "$raw" "committed_total_entries")
  cur_c_single=$(_parse_field "$raw" "committed_single_entry_batches")
  cur_c_iol_commits=$(_parse_field "$raw" "committed_inline_only_batch_commits")
  cur_c_iol_entries=$(_parse_field "$raw" "committed_inline_only_batch_entries")
  cur_conflict=$(_parse_field "$raw" "conflict_pushbacks")
  cur_fallback=$(_parse_field "$raw" "fallback_to_single_count")
  cur_fallback_entries=$(_parse_field "$raw" "fallback_to_single_entries")
  cur_single_txn_puts=$(_parse_field "$raw" "single_txn_puts")

  local d_a_commits d_a_entries d_a_single d_a_iol_commits d_a_iol_entries
  local d_c_commits d_c_entries d_c_single d_c_iol_commits d_c_iol_entries
  local d_conflict d_fallback d_fallback_entries d_single_txn_puts
  d_a_commits=$(( cur_a_commits      - snap_attempted_batch_commits ))
  d_a_entries=$(( cur_a_entries      - snap_attempted_entries_batched ))
  d_a_single=$((  cur_a_single       - snap_attempted_single_entry_batches ))
  d_a_iol_commits=$(( cur_a_iol_commits - snap_attempted_inline_only_batch_commits ))
  d_a_iol_entries=$(( cur_a_iol_entries - snap_attempted_inline_only_batch_entries ))
  d_c_commits=$(( cur_c_commits      - snap_committed_batch_commits ))
  d_c_entries=$(( cur_c_entries      - snap_committed_total_entries ))
  d_c_single=$((  cur_c_single       - snap_committed_single_entry_batches ))
  d_c_iol_commits=$(( cur_c_iol_commits - snap_committed_inline_only_batch_commits ))
  d_c_iol_entries=$(( cur_c_iol_entries - snap_committed_inline_only_batch_entries ))
  d_conflict=$(( cur_conflict        - snap_conflict_pushbacks ))
  d_fallback=$(( cur_fallback        - snap_fallback_to_single_count ))
  d_fallback_entries=$(( cur_fallback_entries - snap_fallback_to_single_entries ))
  d_single_txn_puts=$(( cur_single_txn_puts - snap_single_txn_puts ))

  local avg_bs=0
  [[ "$d_a_commits" -gt 0 ]] && avg_bs=$(( d_a_entries / d_a_commits ))

  # Lifetime running extremes (no admin reset exists for these, so they are
  # cumulative since process start, not a per-phase delta). For Phase 1 this
  # is equivalent to "since Phase 1 started"; for later phases it carries
  # over from earlier phases.
  local cur_a_min_bs cur_a_max_bs
  cur_a_min_bs=$(_parse_field "$raw" "attempted_min_batch_size")
  cur_a_max_bs=$(_parse_field "$raw" "attempted_max_batch_size")

  echo ""
  echo "=== BatchStats: $phase_label ==="
  echo "  total_entries_sent                       : $total_sent"
  echo "  -- attempted --"
  echo "  attempted_batch_commits                  : $d_a_commits"
  echo "  attempted_entries_batched                : $d_a_entries"
  echo "  attempted_avg_batch_size                 : $avg_bs"
  echo "  attempted_min_batch_size (cumulative)    : $cur_a_min_bs"
  echo "  attempted_max_batch_size (cumulative)    : $cur_a_max_bs"
  echo "  attempted_single_entry_batches           : $d_a_single"
  echo "  attempted_inline_only_batch_commits      : $d_a_iol_commits"
  echo "  attempted_inline_only_batch_entries      : $d_a_iol_entries"
  echo "  -- committed --"
  echo "  committed_batch_commits                  : $d_c_commits"
  echo "  committed_total_entries                  : $d_c_entries"
  echo "  committed_single_entry_batches           : $d_c_single"
  echo "  committed_inline_only_batch_commits      : $d_c_iol_commits"
  echo "  committed_inline_only_batch_entries      : $d_c_iol_entries"
  if [[ "$d_conflict" -gt 0 || "$d_fallback" -gt 0 || "$d_single_txn_puts" -gt 0 ]]; then
    echo "  -- misc --"
    echo "  conflict_pushbacks                       : $d_conflict"
    echo "  fallback_to_single_count                 : $d_fallback"
    echo "  fallback_to_single_entries               : $d_fallback_entries"
    echo "  single_txn_puts                          : $d_single_txn_puts"
  fi

  # Sanity checks
  #
  # Note: committed_total_entries already includes committed_inline_only_batch_entries
  # (service_impl.cpp increments committed_total_entries unconditionally on every
  # successful commit, then additionally increments committed_inline_only_batch_entries
  # for the inline-only subset) - so adding the latter on top would double-count it.
  echo ""
  echo "=== BatchStats sanity: $phase_label ==="
  check "committed_total_entries == total_sent" \
        "$d_c_entries" "$total_sent"
  check "committed_batch_commits > 0" \
        "$(( d_c_commits > 0 ? 1 : 0 ))" "1"
  check "attempted_entries_batched + attempted_inline_only_batch_entries == total_sent" \
        "$(( d_a_entries + d_a_iol_entries ))" "$total_sent"
  check "committed_total_entries + single_txn_puts == total_sent" \
        "$(( d_c_entries + d_single_txn_puts ))" "$total_sent"
}

# ---------------------------------------------------------------------------
# run_phase
#
# Usage: run_phase PHASE_LABEL BUCKET MIN_ENTRIES MAX_ENTRIES \
#                 KEY_MIN KEY_MAX DATA_MIN DATA_MAX
#
# DATA_MIN=0 means mixed sizes (0..DATA_MAX); DATA_MIN>0 means fixed range.
# When DATA_MIN=0 and DATA_MAX>0, data size is random in [0, DATA_MAX].
# ---------------------------------------------------------------------------
run_phase() {
  local phase_label="$1" bucket="$2"
  local min_entries="$3" max_entries="$4"
  local key_min="$5"     key_max="$6"
  local data_min="$7"    data_max="$8"
  local put_mode="${9:-run}"

  local phase_tmp="$TMPDIR_ROOT/$(echo "$phase_label" | tr ' :/' '_')"
  mkdir -p "$phase_tmp"

  echo ""
  echo "=== $phase_label  bucket=$bucket  iterations=$NUM_ITERATIONS ==="

  snapshot_stats

  # Arrays accumulating all entries across iterations for final sweep.
  local -a all_keys all_data_files
  all_keys=()
  all_data_files=()

  local total_sent=0
  local iter

  for (( iter=0; iter<NUM_ITERATIONS; iter++ )); do
    local entry_range=$(( max_entries - min_entries + 1 ))
    local n=$(( min_entries + RANDOM % entry_range ))
    total_sent=$(( total_sent + n ))
    local t0 t1 t2 t3 t4
    t0=$(date +%s%3N)

    local iter_tmp="$phase_tmp/iter${iter}"
    mkdir -p "$iter_tmp"

    local -a iter_keys iter_data_files
    iter_keys=()
    iter_data_files=()

    local j
    if [[ "$put_mode" == "sync" ]]; then
      # Each batch (iteration) gets its own subdirectory; files are named
      # after their eventual key so 's5cmd sync batch_dir/ s3://bucket/'
      # uploads them with no path prefix leaking into the stored key.
      # Only safe for short keys (filenames are capped by the filesystem's
      # NAME_MAX, typically 255 bytes).
      local batch_dir="$iter_tmp/batch${iter}"
      mkdir -p "$batch_dir"

      for (( j=0; j<n; j++ )); do
        local key data_file sz

        key=$(make_obj_name "$iter" "$j" "$key_min" "$key_max")
        data_file="$batch_dir/$key"

        if [[ "$data_min" -gt 0 ]]; then
          local data_range=$(( data_max - data_min + 1 ))
          sz=$(( data_min + RANDOM % data_range ))
        else
          sz=$(( RANDOM % (data_max + 1) ))
        fi

        make_data_file "$data_file" "$sz"

        iter_keys+=("$key")
        iter_data_files+=("$data_file")
      done
      t1=$(date +%s%3N)

      # Concurrent PUT via s5cmd sync (uploads the whole batch_dir at once).
      if ! s5cmd --endpoint-url="$ENDPOINT" sync "$batch_dir/" "s3://${bucket}/" >/dev/null 2>&1; then
        echo ""
        fail "iter=$iter s5cmd sync PUT failed  phase='$phase_label'"
      fi
    else
      # Keys can be up to 1024 chars here (exceeds filesystem NAME_MAX), so
      # local filenames must stay short and independent of the key; a
      # cmds_file with explicit 'cp local_path s3://bucket/key' lines lets
      # s5cmd run upload under an arbitrary-length destination key.
      local cmds_file="$iter_tmp/s5cmd_cmds"
      : > "$cmds_file"

      for (( j=0; j<n; j++ )); do
        local key data_file sz

        key=$(make_obj_name "$iter" "$j" "$key_min" "$key_max")
        data_file="$iter_tmp/data_${iter}_${j}"

        if [[ "$data_min" -gt 0 ]]; then
          local data_range=$(( data_max - data_min + 1 ))
          sz=$(( data_min + RANDOM % data_range ))
        else
          sz=$(( RANDOM % (data_max + 1) ))
        fi

        make_data_file "$data_file" "$sz"

        echo "cp $data_file s3://${bucket}/${key}" >> "$cmds_file"
        iter_keys+=("$key")
        iter_data_files+=("$data_file")
      done
      t1=$(date +%s%3N)

      # Concurrent PUT via s5cmd run.
      if ! s5cmd --endpoint-url="$ENDPOINT" run "$cmds_file" >/dev/null 2>&1; then
        echo ""
        fail "iter=$iter s5cmd PUT failed  phase='$phase_label'"
      fi
    fi
    t2=$(date +%s%3N)

    # Concurrent GET via s5cmd, then cmp each got file against original.
    local get_cmds_file="$iter_tmp/s5cmd_get_cmds"
    : > "$get_cmds_file"
    for (( j=0; j<n; j++ )); do
      echo "cp s3://${bucket}/${iter_keys[$j]} $iter_tmp/got_${iter}_${j}" \
        >> "$get_cmds_file"
    done
    if ! s5cmd --endpoint-url="$ENDPOINT" run "$get_cmds_file" >/dev/null 2>&1; then
      echo ""
      fail "iter=$iter s5cmd GET failed  phase='$phase_label'"
    fi
    for (( j=0; j<n; j++ )); do
      local got_file="$iter_tmp/got_${iter}_${j}"
      if ! cmp -s "${iter_data_files[$j]}" "$got_file"; then
        echo ""
        fail "iter=$iter entry=$j body mismatch  key=${iter_keys[$j]}  phase='$phase_label'"
      fi
      all_keys+=("${iter_keys[$j]}")
      all_data_files+=("${iter_data_files[$j]}")
      rm -f "$got_file"
    done
    t3=$(date +%s%3N)

    if [[ "$VERBOSE" -eq 1 ]]; then
      printf "  iter %3d/%d  n=%2d  prep=%3dms  put=%3dms  get=%3dms  total=%4dms\n" \
        "$iter" "$NUM_ITERATIONS" "$n" \
        "$(( t1 - t0 ))" "$(( t2 - t1 ))" "$(( t3 - t2 ))" "$(( t3 - t0 ))"
    fi
  done

  # Final sweep: concurrent GET all objects via s5cmd, then cmp each.
  local total_objs=${#all_keys[@]}
  echo "Final sweep: verifying $total_objs objects..."
  local sweep_cmds="$phase_tmp/sweep_get_cmds"
  : > "$sweep_cmds"
  local idx
  for (( idx=0; idx<total_objs; idx++ )); do
    echo "cp s3://${bucket}/${all_keys[$idx]} $phase_tmp/sweep_got_${idx}" \
      >> "$sweep_cmds"
  done
  if ! s5cmd --endpoint-url="$ENDPOINT" run "$sweep_cmds" >/dev/null 2>&1; then
    fail "final sweep s5cmd GET failed  phase='$phase_label'"
  fi
  for (( idx=0; idx<total_objs; idx++ )); do
    local got_file="$phase_tmp/sweep_got_${idx}"
    if ! cmp -s "${all_data_files[$idx]}" "$got_file"; then
      fail "final sweep body mismatch  key=${all_keys[$idx]}  phase='$phase_label'"
    fi
    rm -f "$got_file"
  done

  print_phase_stats "$phase_label" "$total_sent"

  # Cleanup: delete all objects + bucket in one command.
  echo "Cleanup: s3cmd rb --recursive s3://${bucket} ..."
  s3cmd -c "${S3CMD_CFG}" --host="${INSTANCE0_HOST}" --host-bucket="${INSTANCE0_HOST}" \
    rb --recursive "s3://${bucket}" >/dev/null 2>&1 \
    || fail "s3cmd rb --recursive failed for bucket=$bucket  phase='$phase_label'"

  # Verify bucket is gone.
  local ls_out
  ls_out=$(s3cmd -c "${S3CMD_CFG}" --host="${INSTANCE0_HOST}" --host-bucket="${INSTANCE0_HOST}" \
    ls 2>/dev/null || echo "")
  if echo "$ls_out" | grep -q "$bucket"; then
    echo -e "${RED}  [FAIL] bucket $bucket still present after rb --recursive${NC}"
    fail "bucket not deleted  phase='$phase_label'"
  else
    echo -e "${GREEN}  [OK]   bucket $bucket absent after rb --recursive${NC}"
  fi

  echo -e "${GREEN}PASS: $phase_label${NC}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=== test_batch_mixed.sh ==="

# Push tier config.
tier_cfg_resp=$(admin_cmd "set-tier-config max_inline=256 max_kv_store=4096 batch_size=10 batch_timeout_us=25000 batch_threads=1")
if [[ "$tier_cfg_resp" != OK* ]]; then
  fail "set-tier-config failed: $tier_cfg_resp"
fi
sleep 1

echo "Active tier config: $(admin_cmd "get-tier-config" 2>/dev/null || echo "(query failed)")"

# Create tenant (idempotent).
curl -s -X PUT "${ENDPOINT}/_admin/tenant/batch-mixed-s3-test" >/dev/null 2>&1 || true

# -----------------------------------------------------------------------
# Phase 1: short keys, mixed sizes
# -----------------------------------------------------------------------
BUCKET1=$(make_bucket_name)
create_bucket_tracked "$BUCKET1"

run_phase "Phase 1: short keys" "$BUCKET1" \
  10 10 \
  1 32 \
  0 8192 \
  sync

# -----------------------------------------------------------------------
# Phase 2: long keys [512-1024], mixed sizes
# -----------------------------------------------------------------------
BUCKET2=$(make_bucket_name)
create_bucket_tracked "$BUCKET2"

run_phase "Phase 2: long keys [512-1024]" "$BUCKET2" \
  1 10 \
  512 1024 \
  0 8192 \
  run

# -----------------------------------------------------------------------
# Phase 3: storage-only [376-1024 key, 4097-8192 data], 5-10 entries
# -----------------------------------------------------------------------
BUCKET3=$(make_bucket_name)
create_bucket_tracked "$BUCKET3"

run_phase "Phase 3: storage-only [376-1024 key, 4097-8192 data]" "$BUCKET3" \
  5 10 \
  376 1024 \
  4097 8192 \
  run

echo -e "${GREEN}PASS: test_batch_mixed.sh${NC}"
