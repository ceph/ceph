#!/usr/bin/env bash
# vim: ts=8 sw=2 smarttab
#
# run_seed_to.sh - Run ceph_test_filestore_idempotent_sequence up until an 
# injection point, generating a sequence of operations based on a
# provided seed.
#
# We also perform three additional tests, focused on assessing if 
# replaying a larger chunck of the journal affects the expected store
# behavior. These tests will be performed by increasing the store's
# journal sync interval to a very large value, allowing the store to
# finish execution before the first sync (unless the store runs for
# over 10 hours, case on which the interval variables must be changed
# to an appropriate value). Unless the '--no-journal-test' option is
# specified, we will run the 3 following scenarios:
#  
#  1) journal sync'ing for both stores is good as disabled
#     (we call it '00', for store naming purposes)
#  2) journal sync'ing for store A is as good as disabled
#     (we call it '01', for store naming purposes)
#  3) journal sync'ing for store B is as good as disabled
#     (we call it '10', for store naming purposes)
#
# All log files are also appropriately named accordingly (i.e., a.00.fail,
# a.10.recover, or b.01.clean).
#
# By default, the test will not exit on error, although it will show the
# fail message. This behavior is so defined so we run the whole battery of
# tests, and obtain as many mismatches as possible in one go. We may force
# the test to exit on error by specifying the '--exit-on-error' option.
#
#
set -e

test_opts=""

usage() {
  echo "usage: $1 [options..] <seed> <kill-at>"
  echo 
  echo "options:"
  echo "  -c, --colls <VAL>    # of collections"
  echo "  -o, --objs <VAL>     # of objects"
  echo "  -b, --btrfs <VAL>    seq number for btrfs stores"
  echo "  --no-journal-test    don't perform journal replay tests"
  echo "  -e, --exit-on-error  exit with 1 on error"
  echo "  -v, --valgrind       run commands through valgrind"
  echo
  echo "env vars:"
  echo "  OPTS_STORE           additional opts for both stores"
  echo "  OPTS_STORE_A         additional opts for store A"
  echo "  OPTS_STORE_B         additional opts for store B"
  echo
}

echo $0 $*

die_on_missing_arg() {
  if [[ "$2" == "" ]]; then
    echo "$1: missing required parameter"
    exit 1
  fi
}


required_args=2
obtained_args=0

seed=""
killat=""
on_btrfs=0
on_btrfs_seq=0
journal_test=1
min_sync_interval="36000" # ten hours, yes.
max_sync_interval="36001"
exit_on_error=0
v=""

do_rm() {
  if [[ $on_btrfs -eq 0 ]]; then
    rm -fr $*
  fi
}

set_arg() {
  if [[ $1 -eq 1 ]]; then
    seed=$2
  elif [[ $1 -eq 2 ]]; then
    killat=$2
  else
    echo "error: unknown purpose for '$2'"
    usage $0
    exit 1
  fi
}

while [[ $# -gt 0 ]];
do
  case "$1" in
    -c | --colls)
      die_on_missing_arg "$1" "$2"
      test_opts="$test_opts --test-num-colls $2"
      shift 2
      ;;
    -o | --objs)
      die_on_missing_arg "$1" "$2"
      test_opts="$test_opts --test-num-objs $2"
      shift 2
      ;;
    -h | --help)
      usage $0 ;
      exit 0
      ;;
    -b | --btrfs)
      die_on_missing_arg "$1" "$2"
      on_btrfs=1
      on_btrfs_seq=$2
      shift 2
      ;;
    --no-journal-test)
      journal_test=0
      shift
      ;;
    -e | --exit-on-error)
      exit_on_error=1
      shift
      ;;
    -v | --valgrind)
      v="valgrind --leak-check=full"
      shift
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "$1: unknown option" >&2
      usage $0
      exit 1
      ;;
    *)
      obtained_args=$(($obtained_args+1))
      set_arg $obtained_args $1
      shift
      ;;
  esac
done

if [[ $obtained_args -ne $required_args ]]; then
  echo "error: missing argument"
  usage $0 ;
  exit 1
fi

if [[ "$OPTS_STORE" != "" ]]; then
  test_opts="$test_opts $OPTS_STORE"
fi

test_opts_a="$test_opts"
test_opts_b="$test_opts"

if [[ "$OPTS_STORE_A" != "" ]]; then
    test_opts_a="$test_opts_a $OPTS_STORE_A"
fi
if [[ "$OPTS_STORE_B" != "" ]]; then
    test_opts_b="$test_opts_b $OPTS_STORE_B"
fi

echo seed $seed
echo kill at $killat

# run forever, until $killat...
to=1000000000

#
# store names 
#
# We need these for two reasons:
#  1) if we are running the tests on a btrfs volume, then we need to use
#     a seq number for each run. Being on btrfs means we will fail when
#     removing the store's directories and it's far more simple to just
#     specify differente store names such as 'a.$seq' or 'b.$seq'.
#  
#  2) unless the '--no-journal-test' option is specified, we will run
#     three additional tests for each store, and we will reuse the same
#     command for each one of the runs, but varying the store's name and
#     arguments.
#
store_a="a"
store_b="b"

if [[ $on_btrfs -eq 1 ]]; then
  store_a="$store_a.$on_btrfs_seq"
  store_b="$store_b.$on_btrfs_seq"
fi

total_runs=1

if [[ $journal_test -eq 1 ]]; then
  total_runs=$(($total_runs + 3))
fi

num_runs=0

opt_min_sync="--filestore-min-sync-interval $min_sync_interval"
opt_max_sync="--filestore-max-sync-interval $max_sync_interval"

ret=0

while [[ $num_runs -lt $total_runs ]];
do
  tmp_name_a=$store_a
  tmp_name_b=$store_b
  tmp_opts_a=$test_opts_a
  tmp_opts_b=$test_opts_b

  #
  # We have already tested whether there are diffs when both journals
  # are properly working. Now let's try on three other scenarios:
  #  1) journal sync'ing for both stores is good as disabled
  #     (we call it '00')
  #  2) journal sync'ing for store A is as good as disabled
  #     (we call it '01')
  #  3) journal sync'ing for store B is as good as disabled
  #     (we call it '10')
  #
  if [[ $num_runs -gt 0 && $journal_test -eq 1 ]]; then
    echo "run #$num_runs"
    case $num_runs in
      1)
	tmp_name_a="$tmp_name_a.00"
	tmp_name_b="$tmp_name_b.00"
	tmp_opts_a="$tmp_opts_a $opt_min_sync $opt_max_sync"
	tmp_opts_b="$tmp_opts_b $opt_min_sync $opt_max_sync"
	;;
      2)
	tmp_name_a="$tmp_name_a.01"
	tmp_name_b="$tmp_name_b.01"
	tmp_opts_a="$tmp_opts_a $opt_min_sync $opt_max_sync"
	;;
      3)
	tmp_name_a="$tmp_name_a.10"
	tmp_name_b="$tmp_name_b.10"
	tmp_opts_b="$tmp_opts_b $opt_min_sync $opt_max_sync"
	;;
    esac
  fi

  do_rm $tmp_name_a $tmp_name_a.fail $tmp_name_a.recover
  $v ceph_test_filestore_idempotent_sequence run-sequence-to $to \
    $tmp_name_a $tmp_name_a/journal \
    --test-seed $seed --osd-journal-size 100 \
    --filestore-kill-at $killat $tmp_opts_a \
    --log-file $tmp_name_a.fail --debug-filestore 20 --no-log-to-stderr || true

  stop_at=`ceph_test_filestore_idempotent_sequence get-last-op \
    $tmp_name_a $tmp_name_a/journal \
    --log-file $tmp_name_a.recover \
    --debug-filestore 20 --debug-journal 20 --no-log-to-stderr`

  if [[ "`expr $stop_at - $stop_at 2>/dev/null`" != "0" ]]; then
    echo "error: get-last-op returned '$stop_at'"
    exit 1
  fi

  echo stopped at $stop_at

  do_rm $tmp_name_b $tmp_name_b.clean
  $v ceph_test_filestore_idempotent_sequence run-sequence-to \
    $stop_at $tmp_name_b $tmp_name_b/journal \
    --test-seed $seed --osd-journal-size 100 \
    --log-file $tmp_name_b.clean --debug-filestore 20 --no-log-to-stderr \
    $tmp_opts_b

  if $v ceph_test_filestore_idempotent_sequence diff \
    $tmp_name_a $tmp_name_a/journal $tmp_name_b $tmp_name_b/journal --no-log-to-stderr --log-file $tmp_name_a.diff.log --debug-filestore 20 ; then
      echo OK
  else
    echo "FAIL"
    echo " see:"
    echo "   $tmp_name_a.fail     -- leading up to failure"
    echo "   $tmp_name_a.recover  -- journal replay"
    echo "   $tmp_name_b.clean    -- the clean reference"

    ret=1
    if [[ $exit_on_error -eq 1 ]]; then
      exit 1
    fi
  fi

  num_runs=$(($num_runs+1))
done

exit $ret
