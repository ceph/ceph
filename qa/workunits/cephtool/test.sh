#!/bin/bash -x

source $(dirname $0)/../ceph-helpers.sh

set -e
set -o functrace
PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '
SUDO=${SUDO:-sudo}

function check_no_osd_down()
{
    ! ceph osd dump | grep ' down '
}

function wait_no_osd_down()
{
  for i in $(seq 1 300) ; do
    if ! check_no_osd_down ; then
      echo "waiting for osd(s) to come back up"
      sleep 1
    else
      break
    fi
  done
  check_no_osd_down
}

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

TMPDIR=/tmp/cephtool$$
mkdir $TMPDIR
trap "rm -fr $TMPDIR" 0

TMPFILE=$TMPDIR/test_invalid.$$

#
# retry_eagain max cmd args ...
#
# retry cmd args ... if it exits on error and its output contains the
# string EAGAIN, at most $max times
#
function retry_eagain()
{
    local max=$1
    shift
    local status
    local tmpfile=$TMPDIR/retry_eagain.$$
    local count
    for count in $(seq 1 $max) ; do
        status=0
        "$@" > $tmpfile 2>&1 || status=$?
        if test $status = 0 || 
            ! grep --quiet EAGAIN $tmpfile ; then
            break
        fi
        sleep 1
    done
    if test $count = $max ; then
        echo retried with non zero exit status, $max times: "$@" >&2
    fi
    cat $tmpfile
    rm $tmpfile
    return $status
}

#
# map_enxio_to_eagain cmd arg ...
#
# add EAGAIN to the output of cmd arg ... if the output contains
# ENXIO.
#
function map_enxio_to_eagain()
{
    local status=0
    local tmpfile=$TMPDIR/map_enxio_to_eagain.$$

    "$@" > $tmpfile 2>&1 || status=$?
    if test $status != 0 &&
        grep --quiet ENXIO $tmpfile ; then
        echo "EAGAIN added by $0::map_enxio_to_eagain" >> $tmpfile
    fi
    cat $tmpfile
    rm $tmpfile
    return $status
}

function check_response()
{
	expected_string=$1
	retcode=$2
	expected_retcode=$3
	if [ "$expected_retcode" -a $retcode != $expected_retcode ] ; then
		echo "return code invalid: got $retcode, expected $expected_retcode" >&2
		exit 1
	fi

	if ! grep --quiet -- "$expected_string" $TMPFILE ; then 
		echo "Didn't find $expected_string in output" >&2
		cat $TMPFILE >&2
		exit 1
	fi
}

function get_config_value_or_die()
{
  local target config_opt raw val

  target=$1
  config_opt=$2

  raw="`$SUDO ceph daemon $target config get $config_opt 2>/dev/null`"
  if [[ $? -ne 0 ]]; then
    echo "error obtaining config opt '$config_opt' from '$target': $raw"
    exit 1
  fi

  raw=`echo $raw | sed -e 's/[{} "]//g'`
  val=`echo $raw | cut -f2 -d:`

  echo "$val"
  return 0
}

function expect_config_value()
{
  local target config_opt expected_val val
  target=$1
  config_opt=$2
  expected_val=$3

  val=$(get_config_value_or_die $target $config_opt)

  if [[ "$val" != "$expected_val" ]]; then
    echo "expected '$expected_val', got '$val'"
    exit 1
  fi
}

function ceph_watch_start()
{
    local whatch_opt=--watch

    if [ -n "$1" ]; then
	whatch_opt=--watch-$1
    fi

    CEPH_WATCH_FILE=${TMPDIR}/CEPH_WATCH_$$
    ceph $whatch_opt > $CEPH_WATCH_FILE &
    CEPH_WATCH_PID=$!

    # wait until the "ceph" client is connected and receiving
    # log messages from monitor
    for i in `seq 3`; do
        grep -q "cluster" $CEPH_WATCH_FILE && break
        sleep 1
    done
}

function ceph_watch_wait()
{
    local regexp=$1
    local timeout=30

    if [ -n "$2" ]; then
	timeout=$2
    fi

    for i in `seq ${timeout}`; do
	grep -q "$regexp" $CEPH_WATCH_FILE && break
	sleep 1
    done

    kill $CEPH_WATCH_PID

    if ! grep "$regexp" $CEPH_WATCH_FILE; then
	echo "pattern ${regexp} not found in watch file. Full watch file content:" >&2
	cat $CEPH_WATCH_FILE >&2
	return 1
    fi
}

function test_mon_injectargs()
{
  CEPH_ARGS='--mon_debug_dump_location the.dump' ceph tell osd.0 injectargs --no-osd_enable_op_tracker >& $TMPFILE || return 1
  check_response "osd_enable_op_tracker = 'false'"
  ! grep "the.dump" $TMPFILE || return 1
  ceph tell osd.0 injectargs '--osd_enable_op_tracker --osd_op_history_duration 500' >& $TMPFILE || return 1
  check_response "osd_enable_op_tracker = 'true' osd_op_history_duration = '500'"
  ceph tell osd.0 injectargs --no-osd_enable_op_tracker >& $TMPFILE || return 1
  check_response "osd_enable_op_tracker = 'false'"
  ceph tell osd.0 injectargs -- --osd_enable_op_tracker >& $TMPFILE || return 1
  check_response "osd_enable_op_tracker = 'true'"
  ceph tell osd.0 injectargs -- '--osd_enable_op_tracker --osd_op_history_duration 600' >& $TMPFILE || return 1
  check_response "osd_enable_op_tracker = 'true' osd_op_history_duration = '600'"
  ceph tell osd.0 injectargs -- '--osd_op_history_duration' >& $TMPFILE || return 1
  check_response "Option --osd_op_history_duration requires an argument"

  ceph tell osd.0 injectargs -- '--mon-lease 6' >& $TMPFILE || return 1
  check_response "mon_lease = '6' (unchangeable)"
}

function test_mon_injectargs_SI()
{
  # Test SI units during injectargs and 'config set'
  # We only aim at testing the units are parsed accordingly
  # and don't intend to test whether the options being set
  # actually expect SI units to be passed.
  # Keep in mind that all integer based options (i.e., INT,
  # LONG, U32, U64) will accept SI unit modifiers.
  initial_value=$(get_config_value_or_die "mon.a" "mon_pg_warn_min_objects")
  $SUDO ceph daemon mon.a config set mon_pg_warn_min_objects 10
  expect_config_value "mon.a" "mon_pg_warn_min_objects" 10
  $SUDO ceph daemon mon.a config set mon_pg_warn_min_objects 10K
  expect_config_value "mon.a" "mon_pg_warn_min_objects" 10240
  $SUDO ceph daemon mon.a config set mon_pg_warn_min_objects 1G
  expect_config_value "mon.a" "mon_pg_warn_min_objects" 1073741824
  $SUDO ceph daemon mon.a config set mon_pg_warn_min_objects 10F > $TMPFILE || true
  check_response "'10F': (22) Invalid argument"
  # now test with injectargs
  ceph tell mon.a injectargs '--mon_pg_warn_min_objects 10'
  expect_config_value "mon.a" "mon_pg_warn_min_objects" 10
  ceph tell mon.a injectargs '--mon_pg_warn_min_objects 10K'
  expect_config_value "mon.a" "mon_pg_warn_min_objects" 10240
  ceph tell mon.a injectargs '--mon_pg_warn_min_objects 1G'
  expect_config_value "mon.a" "mon_pg_warn_min_objects" 1073741824
  expect_false ceph tell mon.a injectargs '--mon_pg_warn_min_objects 10F'
  $SUDO ceph daemon mon.a config set mon_pg_warn_min_objects $initial_value
}

function test_tiering_agent()
{
  local slow=slow_eviction
  local fast=fast_eviction
  ceph osd pool create $slow  1 1
  ceph osd pool create $fast  1 1
  ceph osd tier add $slow $fast
  ceph osd tier cache-mode $fast writeback
  ceph osd tier set-overlay $slow $fast
  ceph osd pool set $fast hit_set_type bloom
  rados -p $slow put obj1 /etc/group
  ceph osd pool set $fast target_max_objects  1
  ceph osd pool set $fast hit_set_count 1
  ceph osd pool set $fast hit_set_period 5
  # wait for the object to be evicted from the cache
  local evicted
  evicted=false
  for i in 1 2 4 8 16 32 64 128 256 ; do
      if ! rados -p $fast ls | grep obj1 ; then
          evicted=true
          break
      fi
      sleep $i
  done
  $evicted # assert
  # the object is proxy read and promoted to the cache
  rados -p $slow get obj1 - >/dev/null
  # wait for the promoted object to be evicted again
  evicted=false
  for i in 1 2 4 8 16 32 64 128 256 ; do
      if ! rados -p $fast ls | grep obj1 ; then
          evicted=true
          break
      fi
      sleep $i
  done
  $evicted # assert
  ceph osd tier remove-overlay $slow
  ceph osd tier remove $slow $fast
  ceph osd pool delete $fast $fast --yes-i-really-really-mean-it
  ceph osd pool delete $slow $slow --yes-i-really-really-mean-it
}

function test_tiering()
{
  # tiering
  ceph osd pool create slow 2
  ceph osd pool create slow2 2
  ceph osd pool create cache 2
  ceph osd pool create cache2 2
  ceph osd tier add slow cache
  ceph osd tier add slow cache2
  expect_false ceph osd tier add slow2 cache
  # test some state transitions
  ceph osd tier cache-mode cache writeback
  expect_false ceph osd tier cache-mode cache forward
  ceph osd tier cache-mode cache forward --yes-i-really-mean-it
  expect_false ceph osd tier cache-mode cache readonly
  ceph osd tier cache-mode cache readonly --yes-i-really-mean-it
  expect_false ceph osd tier cache-mode cache forward
  ceph osd tier cache-mode cache forward --yes-i-really-mean-it
  ceph osd tier cache-mode cache none
  ceph osd tier cache-mode cache writeback
  ceph osd tier cache-mode cache proxy
  ceph osd tier cache-mode cache writeback
  expect_false ceph osd tier cache-mode cache none
  expect_false ceph osd tier cache-mode cache readonly --yes-i-really-mean-it
  # test with dirty objects in the tier pool
  # tier pool currently set to 'writeback'
  rados -p cache put /etc/passwd /etc/passwd
  ceph tell osd.\* flush_pg_stats || true
  # 1 dirty object in pool 'cache'
  ceph osd tier cache-mode cache proxy
  expect_false ceph osd tier cache-mode cache none
  expect_false ceph osd tier cache-mode cache readonly --yes-i-really-mean-it
  ceph osd tier cache-mode cache writeback
  # remove object from tier pool
  rados -p cache rm /etc/passwd
  rados -p cache cache-flush-evict-all
  ceph tell osd.\* flush_pg_stats || true
  # no dirty objects in pool 'cache'
  ceph osd tier cache-mode cache proxy
  ceph osd tier cache-mode cache none
  ceph osd tier cache-mode cache readonly --yes-i-really-mean-it
  TRIES=0
  while ! ceph osd pool set cache pg_num 3 --yes-i-really-mean-it 2>$TMPFILE
  do
    grep 'currently creating pgs' $TMPFILE
    TRIES=$(( $TRIES + 1 ))
    test $TRIES -ne 60
    sleep 3
  done
  expect_false ceph osd pool set cache pg_num 4
  ceph osd tier cache-mode cache none
  ceph osd tier set-overlay slow cache
  expect_false ceph osd tier set-overlay slow cache2
  expect_false ceph osd tier remove slow cache
  ceph osd tier remove-overlay slow
  ceph osd tier set-overlay slow cache2
  ceph osd tier remove-overlay slow
  ceph osd tier remove slow cache
  ceph osd tier add slow2 cache
  expect_false ceph osd tier set-overlay slow cache
  ceph osd tier set-overlay slow2 cache
  ceph osd tier remove-overlay slow2
  ceph osd tier remove slow2 cache
  ceph osd tier remove slow cache2

  # make sure a non-empty pool fails
  rados -p cache2 put /etc/passwd /etc/passwd
  while ! ceph df | grep cache2 | grep ' 1 ' ; do
    echo waiting for pg stats to flush
    sleep 2
  done
  expect_false ceph osd tier add slow cache2
  ceph osd tier add slow cache2 --force-nonempty
  ceph osd tier remove slow cache2

  ceph osd pool ls | grep cache2
  ceph osd pool ls -f json-pretty | grep cache2
  ceph osd pool ls detail | grep cache2
  ceph osd pool ls detail -f json-pretty | grep cache2

  ceph osd pool delete cache cache --yes-i-really-really-mean-it
  ceph osd pool delete cache2 cache2 --yes-i-really-really-mean-it

  # make sure we can't clobber snapshot state
  ceph osd pool create snap_base 2
  ceph osd pool create snap_cache 2
  ceph osd pool mksnap snap_cache snapname
  expect_false ceph osd tier add snap_base snap_cache
  ceph osd pool delete snap_base snap_base --yes-i-really-really-mean-it
  ceph osd pool delete snap_cache snap_cache --yes-i-really-really-mean-it

  # make sure we can't create an ec pool tier
  ceph osd pool create eccache 2 2 erasure
  ceph osd pool create repbase 2
  expect_false ceph osd tier add repbase eccache
  ceph osd pool delete repbase repbase --yes-i-really-really-mean-it
  ceph osd pool delete eccache eccache --yes-i-really-really-mean-it

  # convenient add-cache command
  ceph osd pool create cache3 2
  ceph osd tier add-cache slow cache3 1024000
  ceph osd dump | grep cache3 | grep bloom | grep 'false_positive_probability: 0.05' | grep 'target_bytes 1024000' | grep '1200s x4'
  ceph osd tier remove slow cache3 2> $TMPFILE || true
  check_response "EBUSY: tier pool 'cache3' is the overlay for 'slow'; please remove-overlay first"
  ceph osd tier remove-overlay slow
  ceph osd tier remove slow cache3
  ceph osd pool ls | grep cache3
  ceph osd pool delete cache3 cache3 --yes-i-really-really-mean-it
  ! ceph osd pool ls | grep cache3 || exit 1

  ceph osd pool delete slow2 slow2 --yes-i-really-really-mean-it
  ceph osd pool delete slow slow --yes-i-really-really-mean-it

  # check add-cache whether work
  ceph osd pool create datapool 2
  ceph osd pool create cachepool 2
  ceph osd tier add-cache datapool cachepool 1024000
  ceph osd tier cache-mode cachepool writeback
  rados -p datapool put object /etc/passwd
  rados -p cachepool stat object
  rados -p cachepool cache-flush object
  rados -p datapool stat object
  ceph osd tier remove-overlay datapool
  ceph osd tier remove datapool cachepool
  ceph osd pool delete cachepool cachepool --yes-i-really-really-mean-it
  ceph osd pool delete datapool datapool --yes-i-really-really-mean-it

  # protection against pool removal when used as tiers
  ceph osd pool create datapool 2
  ceph osd pool create cachepool 2
  ceph osd tier add-cache datapool cachepool 1024000
  ceph osd pool delete cachepool cachepool --yes-i-really-really-mean-it 2> $TMPFILE || true
  check_response "EBUSY: pool 'cachepool' is a tier of 'datapool'"
  ceph osd pool delete datapool datapool --yes-i-really-really-mean-it 2> $TMPFILE || true
  check_response "EBUSY: pool 'datapool' has tiers cachepool"
  ceph osd tier remove-overlay datapool
  ceph osd tier remove datapool cachepool
  ceph osd pool delete cachepool cachepool --yes-i-really-really-mean-it
  ceph osd pool delete datapool datapool --yes-i-really-really-mean-it

  ## check health check
  ceph osd set notieragent
  ceph osd pool create datapool 2
  ceph osd pool create cache4 2
  ceph osd tier add-cache datapool cache4 1024000
  ceph osd tier cache-mode cache4 writeback
  tmpfile=$(mktemp|grep tmp)
  dd if=/dev/zero of=$tmpfile  bs=4K count=1
  ceph osd pool set cache4 target_max_objects 200
  ceph osd pool set cache4 target_max_bytes 1000000
  rados -p cache4 put foo1 $tmpfile
  rados -p cache4 put foo2 $tmpfile
  rm -f $tmpfile
  ceph tell osd.\* flush_pg_stats || true
  ceph df | grep datapool | grep ' 2 '
  ceph osd tier remove-overlay datapool
  ceph osd tier remove datapool cache4
  ceph osd pool delete cache4 cache4 --yes-i-really-really-mean-it
  ceph osd pool delete datapool datapool --yes-i-really-really-mean-it
  ceph osd unset notieragent


  # make sure 'tier remove' behaves as we expect
  # i.e., removing a tier from a pool that's not its base pool only
  # results in a 'pool foo is now (or already was) not a tier of bar'
  #
  ceph osd pool create basepoolA 2
  ceph osd pool create basepoolB 2
  poolA_id=$(ceph osd dump | grep 'pool.*basepoolA' | awk '{print $2;}')
  poolB_id=$(ceph osd dump | grep 'pool.*basepoolB' | awk '{print $2;}')

  ceph osd pool create cache5 2
  ceph osd pool create cache6 2
  ceph osd tier add basepoolA cache5
  ceph osd tier add basepoolB cache6
  ceph osd tier remove basepoolB cache5 2>&1 | grep 'not a tier of'
  ceph osd dump | grep "pool.*'cache5'" 2>&1 | grep "tier_of[ \t]\+$poolA_id"
  ceph osd tier remove basepoolA cache6 2>&1 | grep 'not a tier of'
  ceph osd dump | grep "pool.*'cache6'" 2>&1 | grep "tier_of[ \t]\+$poolB_id"

  ceph osd tier remove basepoolA cache5 2>&1 | grep 'not a tier of'
  ! ceph osd dump | grep "pool.*'cache5'" 2>&1 | grep "tier_of" || exit 1
  ceph osd tier remove basepoolB cache6 2>&1 | grep 'not a tier of'
  ! ceph osd dump | grep "pool.*'cache6'" 2>&1 | grep "tier_of" || exit 1

  ! ceph osd dump | grep "pool.*'basepoolA'" 2>&1 | grep "tiers" || exit 1
  ! ceph osd dump | grep "pool.*'basepoolB'" 2>&1 | grep "tiers" || exit 1

  ceph osd pool delete cache6 cache6 --yes-i-really-really-mean-it
  ceph osd pool delete cache5 cache5 --yes-i-really-really-mean-it
  ceph osd pool delete basepoolB basepoolB --yes-i-really-really-mean-it
  ceph osd pool delete basepoolA basepoolA --yes-i-really-really-mean-it
}

function test_auth()
{
  ceph auth add client.xx mon allow osd "allow *"
  ceph auth export client.xx >client.xx.keyring
  ceph auth add client.xx -i client.xx.keyring
  rm -f client.xx.keyring
  ceph auth list | grep client.xx
  ceph auth get client.xx | grep caps | grep mon
  ceph auth get client.xx | grep caps | grep osd
  ceph auth get-key client.xx
  ceph auth print-key client.xx
  ceph auth print_key client.xx
  ceph auth caps client.xx osd "allow rw"
  expect_false "ceph auth get client.xx | grep caps | grep mon"
  ceph auth get client.xx | grep osd | grep "allow rw"
  ceph auth export | grep client.xx
  ceph auth export -o authfile
  ceph auth import -i authfile
  ceph auth export -o authfile2
  diff authfile authfile2
  rm authfile authfile2
  ceph auth del client.xx
  expect_false ceph auth get client.xx

  # (almost) interactive mode
  echo -e 'auth add client.xx mon allow osd "allow *"\n' | ceph
  ceph auth get client.xx
  # script mode
  echo 'auth del client.xx' | ceph
  expect_false ceph auth get client.xx

  #
  # get / set auid
  #
  local auid=444
  ceph-authtool --create-keyring --name client.TEST --gen-key --set-uid $auid TEST-keyring
  ceph auth import --in-file TEST-keyring
  rm TEST-keyring
  ceph auth get client.TEST > $TMPFILE
  check_response "auid = $auid"
  ceph --format json-pretty auth get client.TEST > $TMPFILE
  check_response '"auid": '$auid
  ceph auth list > $TMPFILE
  check_response "auid: $auid"
  ceph --format json-pretty auth list > $TMPFILE
  check_response '"auid": '$auid
  ceph auth del client.TEST
}

function test_auth_profiles()
{
  ceph auth add client.xx-profile-ro mon 'allow profile read-only'
  ceph auth add client.xx-profile-rw mon 'allow profile read-write'
  ceph auth add client.xx-profile-rd mon 'allow profile role-definer'

  ceph auth export > client.xx.keyring

  # read-only is allowed all read-only commands (auth excluded)
  ceph -n client.xx-profile-ro -k client.xx.keyring status
  ceph -n client.xx-profile-ro -k client.xx.keyring osd dump
  ceph -n client.xx-profile-ro -k client.xx.keyring pg dump
  ceph -n client.xx-profile-ro -k client.xx.keyring mon dump
  ceph -n client.xx-profile-ro -k client.xx.keyring mds dump
  # read-only gets access denied for rw commands or auth commands
  ceph -n client.xx-profile-ro -k client.xx.keyring log foo >& $TMPFILE || true
  check_response "EACCES: access denied"
  ceph -n client.xx-profile-ro -k client.xx.keyring osd set noout >& $TMPFILE || true
  check_response "EACCES: access denied"
  ceph -n client.xx-profile-ro -k client.xx.keyring auth list >& $TMPFILE || true
  check_response "EACCES: access denied"

  # read-write is allowed for all read-write commands (except auth)
  ceph -n client.xx-profile-rw -k client.xx.keyring status
  ceph -n client.xx-profile-rw -k client.xx.keyring osd dump
  ceph -n client.xx-profile-rw -k client.xx.keyring pg dump
  ceph -n client.xx-profile-rw -k client.xx.keyring mon dump
  ceph -n client.xx-profile-rw -k client.xx.keyring mds dump
  ceph -n client.xx-profile-rw -k client.xx.keyring log foo
  ceph -n client.xx-profile-rw -k client.xx.keyring osd set noout
  ceph -n client.xx-profile-rw -k client.xx.keyring osd unset noout
  # read-write gets access denied for auth commands
  ceph -n client.xx-profile-rw -k client.xx.keyring auth list >& $TMPFILE || true
  check_response "EACCES: access denied"

  # role-definer is allowed RWX 'auth' commands and read-only 'mon' commands
  ceph -n client.xx-profile-rd -k client.xx.keyring auth list
  ceph -n client.xx-profile-rd -k client.xx.keyring auth export
  ceph -n client.xx-profile-rd -k client.xx.keyring auth add client.xx-profile-foo
  ceph -n client.xx-profile-rd -k client.xx.keyring status
  ceph -n client.xx-profile-rd -k client.xx.keyring osd dump >& $TMPFILE || true
  check_response "EACCES: access denied"
  ceph -n client.xx-profile-rd -k client.xx.keyring pg dump >& $TMPFILE || true
  check_response "EACCES: access denied"
  # read-only 'mon' subsystem commands are allowed
  ceph -n client.xx-profile-rd -k client.xx.keyring mon dump
  # but read-write 'mon' commands are not
  ceph -n client.xx-profile-rd -k client.xx.keyring mon add foo 1.1.1.1 >& $TMPFILE || true
  check_response "EACCES: access denied"
  ceph -n client.xx-profile-rd -k client.xx.keyring mds dump >& $TMPFILE || true
  check_response "EACCES: access denied"
  ceph -n client.xx-profile-rd -k client.xx.keyring log foo >& $TMPFILE || true
  check_response "EACCES: access denied"
  ceph -n client.xx-profile-rd -k client.xx.keyring osd set noout >& $TMPFILE || true
  check_response "EACCES: access denied"

  ceph -n client.xx-profile-rd -k client.xx.keyring auth del client.xx-profile-ro
  ceph -n client.xx-profile-rd -k client.xx.keyring auth del client.xx-profile-rw
  
  # add a new role-definer with the existing role-definer
  ceph -n client.xx-profile-rd -k client.xx.keyring \
    auth add client.xx-profile-rd2 mon 'allow profile role-definer'
  ceph -n client.xx-profile-rd -k client.xx.keyring \
    auth export > client.xx.keyring.2
  # remove old role-definer using the new role-definer
  ceph -n client.xx-profile-rd2 -k client.xx.keyring.2 \
    auth del client.xx-profile-rd
  # remove the remaining role-definer with admin
  ceph auth del client.xx-profile-rd2
  rm -f client.xx.keyring client.xx.keyring.2
}

function test_mon_caps()
{
  ceph-authtool --create-keyring $TMPDIR/ceph.client.bug.keyring
  chmod +r  $TMPDIR/ceph.client.bug.keyring
  ceph-authtool  $TMPDIR/ceph.client.bug.keyring -n client.bug --gen-key
  ceph auth add client.bug -i  $TMPDIR/ceph.client.bug.keyring

  rados lspools --keyring $TMPDIR/ceph.client.bug.keyring -n client.bug >& $TMPFILE || true
  check_response "Permission denied"

  rm -rf $TMPDIR/ceph.client.bug.keyring
  ceph auth del client.bug
  ceph-authtool --create-keyring $TMPDIR/ceph.client.bug.keyring
  chmod +r  $TMPDIR/ceph.client.bug.keyring
  ceph-authtool  $TMPDIR/ceph.client.bug.keyring -n client.bug --gen-key
  ceph-authtool -n client.bug --cap mon '' $TMPDIR/ceph.client.bug.keyring
  ceph auth add client.bug -i  $TMPDIR/ceph.client.bug.keyring
  rados lspools --keyring $TMPDIR/ceph.client.bug.keyring -n client.bug >& $TMPFILE || true
  check_response "Permission denied"  
}

function test_mon_misc()
{
  # with and without verbosity
  ceph osd dump | grep '^epoch'
  ceph --concise osd dump | grep '^epoch'

  # df
  ceph df > $TMPFILE
  grep GLOBAL $TMPFILE
  grep -v DIRTY $TMPFILE
  ceph df detail > $TMPFILE
  grep CATEGORY $TMPFILE
  grep DIRTY $TMPFILE
  ceph df --format json > $TMPFILE
  grep 'total_bytes' $TMPFILE
  grep -v 'dirty' $TMPFILE
  ceph df detail --format json > $TMPFILE
  grep 'rd_bytes' $TMPFILE
  grep 'dirty' $TMPFILE
  ceph df --format xml | grep '<total_bytes>'
  ceph df detail --format xml | grep '<rd_bytes>'

  ceph fsid
  ceph health
  ceph health detail
  ceph health --format json-pretty
  ceph health detail --format xml-pretty

  ceph node ls
  for t in mon osd mds ; do
      ceph node ls $t
  done

  ceph_watch_start
  mymsg="this is a test log message $$.$(date)"
  ceph log "$mymsg"
  ceph_watch_wait "$mymsg"

  ceph mon metadata a
  ceph node ls
}

function check_mds_active()
{
    fs_name=$1
    ceph fs get $fs_name | grep active
}

function wait_mds_active()
{
  fs_name=$1
  for i in $(seq 1 300) ; do
      if ! check_mds_active $fs_name ; then
          echo "waiting for an active MDS daemon"
          sleep 5
      else
          break
      fi
  done
  check_mds_active $fs_name
}

function get_mds_gids()
{
    fs_name=$1
    ceph fs get $fs_name --format=json | python -c "import json; import sys; print ' '.join([m['gid'].__str__() for m in json.load(sys.stdin)['mdsmap']['info'].values()])"
}

function fail_all_mds()
{
  fs_name=$1
  ceph fs set $fs_name cluster_down true
  mds_gids=$(get_mds_gids $fs_name)
  for mds_gid in $mds_gids ; do
      ceph mds fail $mds_gid
  done
  if check_mds_active $fs_name ; then
      echo "An active MDS remains, something went wrong"
      ceph fs get $fs_name
      exit -1
  fi

}

function remove_all_fs()
{
  existing_fs=$(ceph fs ls --format=json | python -c "import json; import sys; print ' '.join([fs['name'] for fs in json.load(sys.stdin)])")
  for fs_name in $existing_fs ; do
      echo "Removing fs ${fs_name}..."
      fail_all_mds $fs_name
      echo "Removing existing filesystem '${fs_name}'..."
      ceph fs rm $fs_name --yes-i-really-mean-it
      echo "Removed '${fs_name}'."
  done
}

# So that tests requiring MDS can skip if one is not configured
# in the cluster at all
function mds_exists()
{
    ceph auth list | grep "^mds"
}

function test_mds_tell()
{
  FS_NAME=cephfs
  if ! mds_exists ; then
      echo "Skipping test, no MDS found"
      return
  fi

  remove_all_fs
  ceph osd pool create fs_data 10
  ceph osd pool create fs_metadata 10
  ceph fs new $FS_NAME fs_metadata fs_data
  wait_mds_active $FS_NAME

  # Test injectargs by GID
  old_mds_gids=$(get_mds_gids $FS_NAME)
  echo Old GIDs: $old_mds_gids

  for mds_gid in $old_mds_gids ; do
      ceph tell mds.$mds_gid injectargs "--debug-mds 20"
  done

  # Test respawn by rank
  ceph tell mds.0 respawn
  new_mds_gids=$old_mds_gids
  while [ $new_mds_gids -eq $old_mds_gids ] ; do
      sleep 5
      new_mds_gids=$(get_mds_gids $FS_NAME)
  done
  echo New GIDs: $new_mds_gids

  # Test respawn by ID
  ceph tell mds.a respawn
  new_mds_gids=$old_mds_gids
  while [ $new_mds_gids -eq $old_mds_gids ] ; do
      sleep 5
      new_mds_gids=$(get_mds_gids $FS_NAME)
  done
  echo New GIDs: $new_mds_gids

  remove_all_fs
  ceph osd pool delete fs_data fs_data --yes-i-really-really-mean-it
  ceph osd pool delete fs_metadata fs_metadata --yes-i-really-really-mean-it
}

function test_mon_mds()
{
  FS_NAME=cephfs
  remove_all_fs

  ceph osd pool create fs_data 10
  ceph osd pool create fs_metadata 10
  ceph fs new $FS_NAME fs_metadata fs_data

  ceph fs set $FS_NAME cluster_down true
  ceph fs set $FS_NAME cluster_down false

  # Legacy commands, act on default fs
  ceph mds cluster_down
  ceph mds cluster_up

  ceph mds compat rm_incompat 4
  ceph mds compat rm_incompat 4

  # We don't want any MDSs to be up, their activity can interfere with
  # the "current_epoch + 1" checking below if they're generating updates
  fail_all_mds $FS_NAME

  # Check for default crash_replay_interval set automatically in 'fs new'
  #This may vary based on ceph.conf (e.g., it's 5 in teuthology runs)
  #ceph osd dump | grep fs_data > $TMPFILE
  #check_response "crash_replay_interval 45 "

  ceph mds compat show
  expect_false ceph mds deactivate 2
  ceph mds dump
  ceph fs dump
  ceph fs get $FS_NAME
  for mds_gid in $(get_mds_gids $FS_NAME) ; do
      ceph mds metadata $mds_id
  done
  # XXX mds fail, but how do you undo it?
  mdsmapfile=$TMPDIR/mdsmap.$$
  current_epoch=$(ceph mds getmap -o $mdsmapfile --no-log-to-stderr 2>&1 | grep epoch | sed 's/.*epoch //')
  [ -s $mdsmapfile ]
  rm $mdsmapfile

  ceph osd pool create data2 10
  ceph osd pool create data3 10
  data2_pool=$(ceph osd dump | grep "pool.*'data2'" | awk '{print $2;}')
  data3_pool=$(ceph osd dump | grep "pool.*'data3'" | awk '{print $2;}')
  ceph mds add_data_pool $data2_pool
  ceph mds add_data_pool $data3_pool
  ceph mds add_data_pool 100 >& $TMPFILE || true
  check_response "Error ENOENT"
  ceph mds add_data_pool foobarbaz >& $TMPFILE || true
  check_response "Error ENOENT"
  ceph mds remove_data_pool $data2_pool
  ceph mds remove_data_pool $data3_pool
  ceph osd pool delete data2 data2 --yes-i-really-really-mean-it
  ceph osd pool delete data3 data3 --yes-i-really-really-mean-it
  ceph mds set_max_mds 4
  ceph mds set_max_mds 3
  ceph mds set_max_mds 256
  expect_false ceph mds set_max_mds 257
  ceph mds set max_mds 4
  ceph mds set max_mds 256
  expect_false ceph mds set max_mds 257
  expect_false ceph mds set max_mds asdf
  expect_false ceph mds set inline_data true
  ceph mds set inline_data true --yes-i-really-mean-it
  ceph mds set inline_data yes --yes-i-really-mean-it
  ceph mds set inline_data 1 --yes-i-really-mean-it
  expect_false ceph mds set inline_data --yes-i-really-mean-it
  ceph mds set inline_data false
  ceph mds set inline_data no
  ceph mds set inline_data 0
  expect_false ceph mds set inline_data asdf
  ceph mds set max_file_size 1048576
  expect_false ceph mds set max_file_size 123asdf

  expect_false ceph mds set allow_new_snaps
  expect_false ceph mds set allow_new_snaps true
  ceph mds set allow_new_snaps true --yes-i-really-mean-it
  ceph mds set allow_new_snaps 0
  ceph mds set allow_new_snaps false
  ceph mds set allow_new_snaps no
  expect_false ceph mds set allow_new_snaps taco

  # we should never be able to add EC pools as data or metadata pools
  # create an ec-pool...
  ceph osd pool create mds-ec-pool 10 10 erasure
  set +e
  ceph mds add_data_pool mds-ec-pool 2>$TMPFILE
  check_response 'erasure-code' $? 22
  set -e
  ec_poolnum=$(ceph osd dump | grep "pool.* 'mds-ec-pool" | awk '{print $2;}')
  data_poolnum=$(ceph osd dump | grep "pool.* 'fs_data" | awk '{print $2;}')
  metadata_poolnum=$(ceph osd dump | grep "pool.* 'fs_metadata" | awk '{print $2;}')

  fail_all_mds $FS_NAME

  set +e
  # Check that rmfailed requires confirmation
  expect_false ceph mds rmfailed 0
  ceph mds rmfailed 0 --yes-i-really-mean-it
  set -e

  # Check that `newfs` is no longer permitted
  expect_false ceph mds newfs $metadata_poolnum $data_poolnum --yes-i-really-mean-it 2>$TMPFILE

  # Check that 'fs reset' runs
  ceph fs reset $FS_NAME --yes-i-really-mean-it

  # Check that creating a second FS fails by default
  ceph osd pool create fs_metadata2 10
  ceph osd pool create fs_data2 10
  set +e
  expect_false ceph fs new cephfs2 fs_metadata2 fs_data2
  set -e

  # Check that setting enable_multiple enables creation of second fs
  ceph fs flag set enable_multiple true
  ceph fs new cephfs2 fs_metadata2 fs_data2

  # Clean up multi-fs stuff
  fail_all_mds cephfs2
  ceph fs rm cephfs2 --yes-i-really-mean-it
  ceph osd pool delete fs_metadata2 fs_metadata2 --yes-i-really-really-mean-it
  ceph osd pool delete fs_data2 fs_data2 --yes-i-really-really-mean-it

  fail_all_mds $FS_NAME

  # Clean up to enable subsequent fs new tests
  ceph fs rm $FS_NAME --yes-i-really-mean-it

  set +e
  ceph fs new $FS_NAME fs_metadata mds-ec-pool 2>$TMPFILE
  check_response 'erasure-code' $? 22
  ceph fs new $FS_NAME mds-ec-pool fs_data 2>$TMPFILE
  check_response 'erasure-code' $? 22
  ceph fs new $FS_NAME mds-ec-pool mds-ec-pool 2>$TMPFILE
  check_response 'erasure-code' $? 22
  set -e

  # ... new create a cache tier in front of the EC pool...
  ceph osd pool create mds-tier 2
  ceph osd tier add mds-ec-pool mds-tier
  ceph osd tier set-overlay mds-ec-pool mds-tier
  tier_poolnum=$(ceph osd dump | grep "pool.* 'mds-tier" | awk '{print $2;}')

  # Use of a readonly tier should be forbidden
  ceph osd tier cache-mode mds-tier readonly --yes-i-really-mean-it
  set +e
  ceph fs new $FS_NAME fs_metadata mds-ec-pool 2>$TMPFILE
  check_response 'has a write tier (mds-tier) that is configured to forward' $? 22
  set -e

  # Use of a writeback tier should enable FS creation
  ceph osd tier cache-mode mds-tier writeback
  ceph fs new $FS_NAME fs_metadata mds-ec-pool

  # While a FS exists using the tiered pools, I should not be allowed
  # to remove the tier
  set +e
  ceph osd tier remove-overlay mds-ec-pool 2>$TMPFILE
  check_response 'in use by CephFS' $? 16
  ceph osd tier remove mds-ec-pool mds-tier 2>$TMPFILE
  check_response 'in use by CephFS' $? 16
  set -e

  fail_all_mds $FS_NAME
  ceph fs rm $FS_NAME --yes-i-really-mean-it

  # ... but we should be forbidden from using the cache pool in the FS directly.
  set +e
  ceph fs new $FS_NAME fs_metadata mds-tier 2>$TMPFILE
  check_response 'in use as a cache tier' $? 22
  ceph fs new $FS_NAME mds-tier fs_data 2>$TMPFILE
  check_response 'in use as a cache tier' $? 22
  ceph fs new $FS_NAME mds-tier mds-tier 2>$TMPFILE
  check_response 'in use as a cache tier' $? 22
  set -e

  # Clean up tier + EC pools
  ceph osd tier remove-overlay mds-ec-pool
  ceph osd tier remove mds-ec-pool mds-tier

  # Create a FS using the 'cache' pool now that it's no longer a tier
  ceph fs new $FS_NAME fs_metadata mds-tier

  # We should be forbidden from using this pool as a tier now that
  # it's in use for CephFS
  set +e
  ceph osd tier add mds-ec-pool mds-tier 2>$TMPFILE
  check_response 'in use by CephFS' $? 16
  set -e

  fail_all_mds $FS_NAME
  ceph fs rm $FS_NAME --yes-i-really-mean-it
  ceph osd pool delete mds-ec-pool mds-ec-pool --yes-i-really-really-mean-it

  # Create a FS and check that we can subsequently add a cache tier to it
  ceph fs new $FS_NAME fs_metadata fs_data

  # Adding overlay to FS pool should be permitted, RADOS clients handle this.
  ceph osd tier add fs_metadata mds-tier
  ceph osd tier cache-mode mds-tier writeback
  ceph osd tier set-overlay fs_metadata mds-tier

  # Removing tier should be permitted because the underlying pool is
  # replicated (#11504 case)
  ceph osd tier cache-mode mds-tier proxy
  ceph osd tier remove-overlay fs_metadata
  ceph osd tier remove fs_metadata mds-tier
  ceph osd pool delete mds-tier mds-tier --yes-i-really-really-mean-it

  # Clean up FS
  fail_all_mds $FS_NAME
  ceph fs rm $FS_NAME --yes-i-really-mean-it

  ceph mds stat
  # ceph mds tell mds.a getmap
  # ceph mds rm
  # ceph mds rmfailed
  # ceph mds set_state
  # ceph mds stop

  ceph osd pool delete fs_data fs_data --yes-i-really-really-mean-it
  ceph osd pool delete fs_metadata fs_metadata --yes-i-really-really-mean-it
}

function test_mon_mds_metadata()
{
  local nmons=$(ceph tell 'mon.*' version | grep -c 'version')
  test "$nmons" -gt 0

  ceph mds dump |
  sed -nEe "s/^([0-9]+):.*'([a-z])' mds\\.([0-9]+)\\..*/\\1 \\2 \\3/p" |
  while read gid id rank; do
    ceph mds metadata ${gid} | grep '"hostname":'
    ceph mds metadata ${id} | grep '"hostname":'
    ceph mds metadata ${rank} | grep '"hostname":'

    local n=$(ceph tell 'mon.*' mds metadata ${id} | grep -c '"hostname":')
    test "$n" -eq "$nmons"
  done

  expect_false ceph mds metadata UNKNOWN
}

function test_mon_mon()
{
  # print help message
  ceph --help mon
  # no mon add/remove
  ceph mon dump
  ceph mon getmap -o $TMPDIR/monmap.$$
  [ -s $TMPDIR/monmap.$$ ]
  # ceph mon tell
  ceph mon_status
}

function test_mon_osd()
{
  #
  # osd blacklist
  #
  bl=192.168.0.1:0/1000
  # Escaped form which may appear in JSON output
  bl_json=192.168.0.1:0\\\\/1000
  ceph osd blacklist add $bl
  ceph osd blacklist ls | grep $bl
  ceph osd blacklist ls --format=json-pretty | grep $bl_json
  ceph osd dump --format=json-pretty | grep $bl
  ceph osd dump | grep "^blacklist $bl"
  ceph osd blacklist rm $bl
  expect_false "ceph osd blacklist ls | grep $bl"

  bl=192.168.0.1
  # test without nonce, invalid nonce
  ceph osd blacklist add $bl
  ceph osd blacklist ls | grep $bl
  ceph osd blacklist rm $bl
  expect_false "ceph osd blacklist ls | grep $bl"
  expect_false "ceph osd blacklist $bl/-1"
  expect_false "ceph osd blacklist $bl/foo"

  # Test `clear`
  ceph osd blacklist add $bl
  ceph osd blacklist ls | grep $bl
  ceph osd blacklist clear
  expect_false "ceph osd blacklist ls | grep $bl"

  #
  # osd crush
  #
  ceph osd crush reweight-all
  ceph osd crush tunables legacy
  ceph osd crush show-tunables | grep argonaut
  ceph osd crush tunables bobtail
  ceph osd crush show-tunables | grep bobtail
  ceph osd crush tunables firefly
  ceph osd crush show-tunables | grep firefly

  ceph osd crush set-tunable straw_calc_version 0
  ceph osd crush get-tunable straw_calc_version | grep 0
  ceph osd crush set-tunable straw_calc_version 1
  ceph osd crush get-tunable straw_calc_version | grep 1

  #
  # osd scrub
  #
  # how do I tell when these are done?
  ceph osd scrub 0
  ceph osd deep-scrub 0
  ceph osd repair 0

  for f in noup nodown noin noout noscrub nodeep-scrub nobackfill norebalance norecover notieragent full sortbitwise
  do
    ceph osd set $f
    ceph osd unset $f
  done
  ceph osd set sortbitwise  # new backends can't handle nibblewise
  expect_false ceph osd set bogus
  expect_false ceph osd unset bogus

  ceph osd set noup
  ceph osd down 0
  ceph osd dump | grep 'osd.0 down'
  ceph osd unset noup
  for ((i=0; i < 1000; i++)); do
    if ! ceph osd dump | grep 'osd.0 up'; then
      echo "waiting for osd.0 to come back up"
      sleep 1
    else
      break
    fi
  done
  ceph osd dump | grep 'osd.0 up'

  ceph osd thrash 0

  ceph osd dump | grep 'osd.0 up'
  ceph osd find 1
  ceph --format plain osd find 1 # falls back to json-pretty
  ceph osd metadata 1 | grep 'distro'
  ceph --format plain osd metadata 1 | grep 'distro' # falls back to json-pretty
  ceph osd out 0
  ceph osd dump | grep 'osd.0.*out'
  ceph osd in 0
  ceph osd dump | grep 'osd.0.*in'
  ceph osd find 0

  f=$TMPDIR/map.$$
  ceph osd getcrushmap -o $f
  [ -s $f ]
  ceph osd setcrushmap -i $f
  rm $f
  ceph osd getmap -o $f
  [ -s $f ]
  rm $f
  save=$(ceph osd getmaxosd | sed -e 's/max_osd = //' -e 's/ in epoch.*//')
  [ "$save" -gt 0 ]
  ceph osd setmaxosd $((save - 1)) 2>&1 | grep 'EBUSY'
  ceph osd setmaxosd 10
  ceph osd getmaxosd | grep 'max_osd = 10'
  ceph osd setmaxosd $save
  ceph osd getmaxosd | grep "max_osd = $save"

  for id in `ceph osd ls` ; do
    retry_eagain 5 map_enxio_to_eagain ceph tell osd.$id version
  done

  ceph osd rm 0 2>&1 | grep 'EBUSY'

  local old_osds=$(echo $(ceph osd ls))
  id=`ceph osd create`
  ceph osd find $id
  ceph osd lost $id --yes-i-really-mean-it
  expect_false ceph osd setmaxosd $id
  local new_osds=$(echo $(ceph osd ls))
  for id in $(echo $new_osds | sed -e "s/$old_osds//") ; do
      ceph osd rm $id
  done

  uuid=`uuidgen`
  id=`ceph osd create $uuid`
  id2=`ceph osd create $uuid`
  [ "$id" = "$id2" ]
  ceph osd rm $id

  ceph --help osd

  # reset max_osd.
  ceph osd setmaxosd $id
  ceph osd getmaxosd | grep "max_osd = $save"
  local max_osd=$save

  ceph osd create $uuid 0 2>&1 | grep 'EINVAL'
  ceph osd create $uuid $((max_osd - 1)) 2>&1 | grep 'EINVAL'

  id=`ceph osd create $uuid $max_osd`
  [ "$id" = "$max_osd" ]
  ceph osd find $id
  max_osd=$((max_osd + 1))
  ceph osd getmaxosd | grep "max_osd = $max_osd"

  ceph osd create $uuid $((id - 1)) 2>&1 | grep 'EINVAL'
  ceph osd create $uuid $((id + 1)) 2>&1 | grep 'EINVAL'
  id2=`ceph osd create $uuid`
  [ "$id" = "$id2" ]
  id2=`ceph osd create $uuid $id`
  [ "$id" = "$id2" ]

  uuid=`uuidgen`
  local gap_start=$max_osd
  id=`ceph osd create $uuid $((gap_start + 100))`
  [ "$id" = "$((gap_start + 100))" ]
  max_osd=$((id + 1))
  ceph osd getmaxosd | grep "max_osd = $max_osd"

  ceph osd create $uuid $gap_start 2>&1 | grep 'EINVAL'

  #
  # When CEPH_CLI_TEST_DUP_COMMAND is set, osd create
  # is repeated and consumes two osd id, not just one.
  #
  local next_osd
  if test "$CEPH_CLI_TEST_DUP_COMMAND" ; then
      next_osd=$((gap_start + 1))
  else
      next_osd=$gap_start
  fi
  id=`ceph osd create`
  [ "$id" = "$next_osd" ]

  next_osd=$((id + 1))
  id=`ceph osd create $(uuidgen)`
  [ "$id" = "$next_osd" ]

  next_osd=$((id + 1))
  id=`ceph osd create $(uuidgen) $next_osd`
  [ "$id" = "$next_osd" ]

  local new_osds=$(echo $(ceph osd ls))
  for id in $(echo $new_osds | sed -e "s/$old_osds//") ; do
      [ $id -ge $save ]
      ceph osd rm $id
  done
  ceph osd setmaxosd $save

  ceph osd ls
  ceph osd pool create data 10
  ceph osd lspools | grep data
  ceph osd map data foo | grep 'pool.*data.*object.*foo.*pg.*up.*acting'
  ceph osd map data foo namespace| grep 'pool.*data.*object.*namespace/foo.*pg.*up.*acting'
  ceph osd pool delete data data --yes-i-really-really-mean-it

  ceph osd pause
  ceph osd dump | grep 'flags pauserd,pausewr'
  ceph osd unpause

  ceph osd tree
  ceph osd perf
  ceph osd blocked-by

  ceph osd stat | grep up,
}

function test_mon_osd_pool()
{
  #
  # osd pool
  #
  ceph osd pool create data 10
  ceph osd pool mksnap data datasnap
  rados -p data lssnap | grep datasnap
  ceph osd pool rmsnap data datasnap
  ceph osd pool delete data data --yes-i-really-really-mean-it

  ceph osd pool create data2 10
  ceph osd pool rename data2 data3
  ceph osd lspools | grep data3
  ceph osd pool delete data3 data3 --yes-i-really-really-mean-it

  ceph osd pool create replicated 12 12 replicated
  ceph osd pool create replicated 12 12 replicated
  ceph osd pool create replicated 12 12 # default is replicated
  ceph osd pool create replicated 12    # default is replicated, pgp_num = pg_num
  # should fail because the type is not the same
  expect_false ceph osd pool create replicated 12 12 erasure
  ceph osd lspools | grep replicated
  ceph osd pool delete replicated replicated --yes-i-really-really-mean-it
}

function test_mon_osd_pool_quota()
{
  #
  # test osd pool set/get quota
  #

  # create tmp pool
  ceph osd pool create tmp-quota-pool 36
  #
  # set erroneous quotas
  #
  expect_false ceph osd pool set-quota tmp-quota-pool max_fooness 10
  expect_false ceph osd pool set-quota tmp-quota-pool max_bytes -1
  expect_false ceph osd pool set-quota tmp-quota-pool max_objects aaa
  #
  # set valid quotas
  #
  ceph osd pool set-quota tmp-quota-pool max_bytes 10
  ceph osd pool set-quota tmp-quota-pool max_objects 10M
  #
  # get quotas
  #
  ceph osd pool get-quota tmp-quota-pool | grep 'max bytes.*10B'
  ceph osd pool get-quota tmp-quota-pool | grep 'max objects.*10240k objects'
  #
  # get quotas in json-pretty format
  #
  ceph osd pool get-quota tmp-quota-pool --format=json-pretty | \
    grep '"quota_max_objects":.*10485760'
  ceph osd pool get-quota tmp-quota-pool --format=json-pretty | \
    grep '"quota_max_bytes":.*10'
  #
  # reset pool quotas
  #
  ceph osd pool set-quota tmp-quota-pool max_bytes 0
  ceph osd pool set-quota tmp-quota-pool max_objects 0
  #
  # test N/A quotas
  #
  ceph osd pool get-quota tmp-quota-pool | grep 'max bytes.*N/A'
  ceph osd pool get-quota tmp-quota-pool | grep 'max objects.*N/A'
  #
  # cleanup tmp pool
  ceph osd pool delete tmp-quota-pool tmp-quota-pool --yes-i-really-really-mean-it
}

function test_mon_pg()
{
  ceph pg debug unfound_objects_exist
  ceph pg debug degraded_pgs_exist
  ceph pg deep-scrub 0.0
  ceph pg dump
  ceph pg dump pgs_brief --format=json
  ceph pg dump pgs --format=json
  ceph pg dump pools --format=json
  ceph pg dump osds --format=json
  ceph pg dump sum --format=json
  ceph pg dump all --format=json
  ceph pg dump pgs_brief osds --format=json
  ceph pg dump pools osds pgs_brief --format=json
  ceph pg dump_json
  ceph pg dump_pools_json
  ceph pg dump_stuck inactive
  ceph pg dump_stuck unclean
  ceph pg dump_stuck stale
  ceph pg dump_stuck undersized
  ceph pg dump_stuck degraded
  ceph pg ls
  ceph pg ls 0
  ceph pg ls stale
  ceph pg ls active stale repair recovering
  ceph pg ls 0 active
  ceph pg ls 0 active stale
  ceph pg ls-by-primary osd.0
  ceph pg ls-by-primary osd.0 0
  ceph pg ls-by-primary osd.0 active
  ceph pg ls-by-primary osd.0 active stale
  ceph pg ls-by-primary osd.0 0 active stale
  ceph pg ls-by-osd osd.0
  ceph pg ls-by-osd osd.0 0
  ceph pg ls-by-osd osd.0 active
  ceph pg ls-by-osd osd.0 active stale
  ceph pg ls-by-osd osd.0 0 active stale
  ceph pg ls-by-pool rbd
  ceph pg ls-by-pool rbd active stale
  # can't test this...
  # ceph pg force_create_pg
  ceph pg getmap -o $TMPDIR/map.$$
  [ -s $TMPDIR/map.$$ ]
  ceph pg map 0.0 | grep acting
  ceph pg repair 0.0
  ceph pg scrub 0.0

  ceph pg set_full_ratio 0.90
  ceph pg dump --format=plain | grep '^full_ratio 0.9'
  ceph pg set_full_ratio 0.95
  ceph pg set_nearfull_ratio 0.90
  ceph pg dump --format=plain | grep '^nearfull_ratio 0.9'
  ceph pg set_nearfull_ratio 0.85
  ceph pg stat | grep 'pgs:'
  ceph pg 0.0 query
  ceph tell 0.0 query
  ceph quorum enter
  ceph quorum_status
  ceph report | grep osd_stats
  ceph status
  ceph -s

  #
  # tell osd version
  #
  ceph tell osd.0 version
  expect_false ceph tell osd.9999 version 
  expect_false ceph tell osd.foo version

  # back to pg stuff

  ceph tell osd.0 dump_pg_recovery_stats | grep Started

  ceph osd reweight 0 0.9
  expect_false ceph osd reweight 0 -1
  ceph osd reweight 0 1

  ceph osd primary-affinity osd.0 .9
  expect_false ceph osd primary-affinity osd.0 -2
  ceph osd primary-affinity osd.0 1

  ceph osd pg-temp 0.0 0 1 2
  ceph osd pg-temp 0.0 1 0 2
  expect_false ceph osd pg-temp asdf qwer
  expect_false ceph osd pg-temp 0.0 asdf
  expect_false ceph osd pg-temp 0.0

  # don't test ceph osd primary-temp for now
}

function test_mon_osd_pool_set()
{
  TEST_POOL_GETSET=pool_getset
  ceph osd pool create $TEST_POOL_GETSET 1
  wait_for_clean
  ceph osd pool get $TEST_POOL_GETSET all

  for s in pg_num pgp_num size min_size crash_replay_interval crush_ruleset; do
    ceph osd pool get $TEST_POOL_GETSET $s
  done

  old_size=$(ceph osd pool get $TEST_POOL_GETSET size | sed -e 's/size: //')
  (( new_size = old_size + 1 ))
  ceph osd pool set $TEST_POOL_GETSET size $new_size
  ceph osd pool get $TEST_POOL_GETSET size | grep "size: $new_size"
  ceph osd pool set $TEST_POOL_GETSET size $old_size

  ceph osd pool create pool_erasure 1 1 erasure
  wait_for_clean
  set +e
  ceph osd pool set pool_erasure size 4444 2>$TMPFILE
  check_response 'not change the size'
  set -e
  ceph osd pool get pool_erasure erasure_code_profile

  auid=5555
  ceph osd pool set $TEST_POOL_GETSET auid $auid
  ceph osd pool get $TEST_POOL_GETSET auid | grep $auid
  ceph --format=xml osd pool get $TEST_POOL_GETSET auid | grep $auid
  ceph osd pool set $TEST_POOL_GETSET auid 0

  for flag in hashpspool nodelete nopgchange nosizechange write_fadvise_dontneed noscrub nodeep-scrub; do
      ceph osd pool set $TEST_POOL_GETSET $flag false
      ceph osd pool get $TEST_POOL_GETSET $flag | grep "$flag: false"
      ceph osd pool set $TEST_POOL_GETSET $flag true
      ceph osd pool get $TEST_POOL_GETSET $flag | grep "$flag: true"
      ceph osd pool set $TEST_POOL_GETSET $flag 1
      ceph osd pool get $TEST_POOL_GETSET $flag | grep "$flag: true"
      ceph osd pool set $TEST_POOL_GETSET $flag 0
      ceph osd pool get $TEST_POOL_GETSET $flag | grep "$flag: false"
      expect_false ceph osd pool set $TEST_POOL_GETSET $flag asdf
      expect_false ceph osd pool set $TEST_POOL_GETSET $flag 2
  done

  expect_false "ceph osd pool get $TEST_POOL_GETSET scrub_min_interval | grep '.'"
  ceph osd pool set $TEST_POOL_GETSET scrub_min_interval 123456
  ceph osd pool get $TEST_POOL_GETSET scrub_min_interval | grep 'scrub_min_interval: 123456'
  ceph osd pool set $TEST_POOL_GETSET scrub_min_interval 0
  expect_false "ceph osd pool get $TEST_POOL_GETSET scrub_min_interval | grep '.'"

  expect_false "ceph osd pool get $TEST_POOL_GETSET scrub_max_interval | grep '.'"
  ceph osd pool set $TEST_POOL_GETSET scrub_max_interval 123456
  ceph osd pool get $TEST_POOL_GETSET scrub_max_interval | grep 'scrub_max_interval: 123456'
  ceph osd pool set $TEST_POOL_GETSET scrub_max_interval 0
  expect_false "ceph osd pool get $TEST_POOL_GETSET scrub_max_interval | grep '.'"

  expect_false "ceph osd pool get $TEST_POOL_GETSET deep_scrub_interval | grep '.'"
  ceph osd pool set $TEST_POOL_GETSET deep_scrub_interval 123456
  ceph osd pool get $TEST_POOL_GETSET deep_scrub_interval | grep 'deep_scrub_interval: 123456'
  ceph osd pool set $TEST_POOL_GETSET deep_scrub_interval 0
  expect_false "ceph osd pool get $TEST_POOL_GETSET deep_scrub_interval | grep '.'"

  expect_false "ceph osd pool get $TEST_POOL_GETSET recovery_priority | grep '.'"
  ceph osd pool set $TEST_POOL_GETSET recovery_priority 5 
  ceph osd pool get $TEST_POOL_GETSET recovery_priority | grep 'recovery_priority: 5'
  ceph osd pool set $TEST_POOL_GETSET recovery_priority 0
  expect_false "ceph osd pool get $TEST_POOL_GETSET recovery_priority | grep '.'"

  expect_false "ceph osd pool get $TEST_POOL_GETSET recovery_op_priority | grep '.'"
  ceph osd pool set $TEST_POOL_GETSET recovery_op_priority 5 
  ceph osd pool get $TEST_POOL_GETSET recovery_op_priority | grep 'recovery_op_priority: 5'
  ceph osd pool set $TEST_POOL_GETSET recovery_op_priority 0
  expect_false "ceph osd pool get $TEST_POOL_GETSET recovery_op_priority | grep '.'"

  expect_false "ceph osd pool get $TEST_POOL_GETSET scrub_priority | grep '.'"
  ceph osd pool set $TEST_POOL_GETSET scrub_priority 5 
  ceph osd pool get $TEST_POOL_GETSET scrub_priority | grep 'scrub_priority: 5'
  ceph osd pool set $TEST_POOL_GETSET scrub_priority 0
  expect_false "ceph osd pool get $TEST_POOL_GETSET scrub_priority | grep '.'"

  ceph osd pool set $TEST_POOL_GETSET nopgchange 1
  expect_false ceph osd pool set $TEST_POOL_GETSET pg_num 10
  expect_false ceph osd pool set $TEST_POOL_GETSET pgp_num 10
  ceph osd pool set $TEST_POOL_GETSET nopgchange 0
  ceph osd pool set $TEST_POOL_GETSET pg_num 10
  wait_for_clean
  ceph osd pool set $TEST_POOL_GETSET pgp_num 10

  ceph osd pool set $TEST_POOL_GETSET nosizechange 1
  expect_false ceph osd pool set $TEST_POOL_GETSET size 2
  expect_false ceph osd pool set $TEST_POOL_GETSET min_size 2
  ceph osd pool set $TEST_POOL_GETSET nosizechange 0
  ceph osd pool set $TEST_POOL_GETSET size 2
  wait_for_clean
  ceph osd pool set $TEST_POOL_GETSET min_size 2

  ceph osd pool set $TEST_POOL_GETSET nodelete 1
  expect_false ceph osd pool delete $TEST_POOL_GETSET $TEST_POOL_GETSET --yes-i-really-really-mean-it
  ceph osd pool set $TEST_POOL_GETSET nodelete 0
  ceph osd pool delete $TEST_POOL_GETSET $TEST_POOL_GETSET --yes-i-really-really-mean-it

  ceph osd pool get rbd crush_ruleset | grep 'crush_ruleset: 0'
}

function test_mon_osd_tiered_pool_set()
{
  # this is really a tier pool
  ceph osd pool create real-tier 2
  ceph osd tier add rbd real-tier

  ceph osd pool set real-tier hit_set_type explicit_hash
  ceph osd pool get real-tier hit_set_type | grep "hit_set_type: explicit_hash"
  ceph osd pool set real-tier hit_set_type explicit_object
  ceph osd pool get real-tier hit_set_type | grep "hit_set_type: explicit_object"
  ceph osd pool set real-tier hit_set_type bloom
  ceph osd pool get real-tier hit_set_type | grep "hit_set_type: bloom"
  expect_false ceph osd pool set real-tier hit_set_type i_dont_exist
  ceph osd pool set real-tier hit_set_period 123
  ceph osd pool get real-tier hit_set_period | grep "hit_set_period: 123"
  ceph osd pool set real-tier hit_set_count 12
  ceph osd pool get real-tier hit_set_count | grep "hit_set_count: 12"
  ceph osd pool set real-tier hit_set_fpp .01
  ceph osd pool get real-tier hit_set_fpp | grep "hit_set_fpp: 0.01"

  ceph osd pool set real-tier target_max_objects 123
  ceph osd pool get real-tier target_max_objects | \
    grep 'target_max_objects:[ \t]\+123'
  ceph osd pool set real-tier target_max_bytes 123456
  ceph osd pool get real-tier target_max_bytes | \
    grep 'target_max_bytes:[ \t]\+123456'
  ceph osd pool set real-tier cache_target_dirty_ratio .123
  ceph osd pool get real-tier cache_target_dirty_ratio | \
    grep 'cache_target_dirty_ratio:[ \t]\+0.123'
  expect_false ceph osd pool set real-tier cache_target_dirty_ratio -.2
  expect_false ceph osd pool set real-tier cache_target_dirty_ratio 1.1
  ceph osd pool set real-tier cache_target_dirty_high_ratio .123
  ceph osd pool get real-tier cache_target_dirty_high_ratio | \
    grep 'cache_target_dirty_high_ratio:[ \t]\+0.123'
  expect_false ceph osd pool set real-tier cache_target_dirty_high_ratio -.2
  expect_false ceph osd pool set real-tier cache_target_dirty_high_ratio 1.1
  ceph osd pool set real-tier cache_target_full_ratio .123
  ceph osd pool get real-tier cache_target_full_ratio | \
    grep 'cache_target_full_ratio:[ \t]\+0.123'
  ceph osd dump -f json-pretty | grep '"cache_target_full_ratio_micro": 123000'
  ceph osd pool set real-tier cache_target_full_ratio 1.0
  ceph osd pool set real-tier cache_target_full_ratio 0
  expect_false ceph osd pool set real-tier cache_target_full_ratio 1.1
  ceph osd pool set real-tier cache_min_flush_age 123
  ceph osd pool get real-tier cache_min_flush_age | \
    grep 'cache_min_flush_age:[ \t]\+123'
  ceph osd pool set real-tier cache_min_evict_age 234
  ceph osd pool get real-tier cache_min_evict_age | \
    grep 'cache_min_evict_age:[ \t]\+234'

  # this is not a tier pool
  ceph osd pool create fake-tier 2
  wait_for_clean

  expect_false ceph osd pool set fake-tier hit_set_type explicit_hash
  expect_false ceph osd pool get fake-tier hit_set_type
  expect_false ceph osd pool set fake-tier hit_set_type explicit_object
  expect_false ceph osd pool get fake-tier hit_set_type
  expect_false ceph osd pool set fake-tier hit_set_type bloom
  expect_false ceph osd pool get fake-tier hit_set_type
  expect_false ceph osd pool set fake-tier hit_set_type i_dont_exist
  expect_false ceph osd pool set fake-tier hit_set_period 123
  expect_false ceph osd pool get fake-tier hit_set_period
  expect_false ceph osd pool set fake-tier hit_set_count 12
  expect_false ceph osd pool get fake-tier hit_set_count
  expect_false ceph osd pool set fake-tier hit_set_fpp .01
  expect_false ceph osd pool get fake-tier hit_set_fpp

  expect_false ceph osd pool set fake-tier target_max_objects 123
  expect_false ceph osd pool get fake-tier target_max_objects
  expect_false ceph osd pool set fake-tier target_max_bytes 123456
  expect_false ceph osd pool get fake-tier target_max_bytes
  expect_false ceph osd pool set fake-tier cache_target_dirty_ratio .123
  expect_false ceph osd pool get fake-tier cache_target_dirty_ratio
  expect_false ceph osd pool set fake-tier cache_target_dirty_ratio -.2
  expect_false ceph osd pool set fake-tier cache_target_dirty_ratio 1.1
  expect_false ceph osd pool set fake-tier cache_target_dirty_high_ratio .123
  expect_false ceph osd pool get fake-tier cache_target_dirty_high_ratio
  expect_false ceph osd pool set fake-tier cache_target_dirty_high_ratio -.2
  expect_false ceph osd pool set fake-tier cache_target_dirty_high_ratio 1.1
  expect_false ceph osd pool set fake-tier cache_target_full_ratio .123
  expect_false ceph osd pool get fake-tier cache_target_full_ratio
  expect_false ceph osd pool set fake-tier cache_target_full_ratio 1.0
  expect_false ceph osd pool set fake-tier cache_target_full_ratio 0
  expect_false ceph osd pool set fake-tier cache_target_full_ratio 1.1
  expect_false ceph osd pool set fake-tier cache_min_flush_age 123
  expect_false ceph osd pool get fake-tier cache_min_flush_age
  expect_false ceph osd pool set fake-tier cache_min_evict_age 234
  expect_false ceph osd pool get fake-tier cache_min_evict_age

  ceph osd tier remove rbd real-tier
  ceph osd pool delete real-tier real-tier --yes-i-really-really-mean-it
  ceph osd pool delete fake-tier fake-tier --yes-i-really-really-mean-it
}

function test_mon_osd_erasure_code()
{

  ceph osd erasure-code-profile set fooprofile a=b c=d
  ceph osd erasure-code-profile set fooprofile a=b c=d
  expect_false ceph osd erasure-code-profile set fooprofile a=b c=d e=f
  ceph osd erasure-code-profile set fooprofile a=b c=d e=f --force
  ceph osd erasure-code-profile set fooprofile a=b c=d e=f
  expect_false ceph osd erasure-code-profile set fooprofile a=b c=d e=f g=h
  #
  # cleanup by removing profile 'fooprofile'
  ceph osd erasure-code-profile rm fooprofile
}

function test_mon_osd_misc()
{
  set +e

  # expect error about missing 'pool' argument
  ceph osd map 2>$TMPFILE; check_response 'pool' $? 22

  # expect error about unused argument foo
  ceph osd ls foo 2>$TMPFILE; check_response 'unused' $? 22 

  # expect "not in range" for invalid full ratio
  ceph pg set_full_ratio 95 2>$TMPFILE; check_response 'not in range' $? 22

  # expect "not in range" for invalid overload percentage
  ceph osd reweight-by-utilization 80 2>$TMPFILE; check_response 'higher than 100' $? 22

  set -e

  ceph osd reweight-by-utilization 110
  ceph osd reweight-by-utilization 110 .5
  ceph osd test-reweight-by-utilization 110 .5 --no-increasing
  ceph osd reweight-by-pg 110
  ceph osd test-reweight-by-pg 110 .5
  ceph osd reweight-by-pg 110 rbd
  ceph osd reweight-by-pg 110 .5 rbd
  expect_false ceph osd reweight-by-pg 110 boguspoolasdfasdfasdf
}

function test_mon_heap_profiler()
{
  do_test=1
  set +e
  # expect 'heap' commands to be correctly parsed
  ceph heap stats 2>$TMPFILE
  if [[ $? -eq 22 && `grep 'tcmalloc not enabled' $TMPFILE` ]]; then
    echo "tcmalloc not enabled; skip heap profiler test"
    do_test=0
  fi
  set -e

  [[ $do_test -eq 0 ]] && return 0

  ceph heap start_profiler
  ceph heap dump
  ceph heap stop_profiler
  ceph heap release
}

function test_osd_bench()
{
  # test osd bench limits
  # As we should not rely on defaults (as they may change over time),
  # lets inject some values and perform some simple tests
  # max iops: 10              # 100 IOPS
  # max throughput: 10485760  # 10MB/s
  # max block size: 2097152   # 2MB
  # duration: 10              # 10 seconds

  ceph tell osd.0 injectargs "\
    --osd-bench-duration 10 \
    --osd-bench-max-block-size 2097152 \
    --osd-bench-large-size-max-throughput 10485760 \
    --osd-bench-small-size-max-iops 10"

  # anything with a bs larger than 2097152  must fail
  expect_false ceph tell osd.0 bench 1 2097153
  # but using 'osd_bench_max_bs' must succeed
  ceph tell osd.0 bench 1 2097152

  # we assume 1MB as a large bs; anything lower is a small bs
  # for a 4096 bytes bs, for 10 seconds, we are limited by IOPS
  # max count: 409600 (bytes)

  # more than max count must not be allowed
  expect_false ceph tell osd.0 bench 409601 4096
  # but 409600 must be succeed
  ceph tell osd.0 bench 409600 4096

  # for a large bs, we are limited by throughput.
  # for a 2MB block size for 10 seconds, assuming 10MB/s throughput,
  # the max count will be (10MB * 10s) = 100MB
  # max count: 104857600 (bytes)

  # more than max count must not be allowed
  expect_false ceph tell osd.0 bench 104857601 2097152
  # up to max count must be allowed
  ceph tell osd.0 bench 104857600 2097152
}

function test_osd_negative_filestore_merge_threshold()
{
  $SUDO ceph daemon osd.0 config set filestore_merge_threshold -1
  expect_config_value "osd.0" "filestore_merge_threshold" -1
}

function test_mon_tell()
{
  ceph tell mon.a version
  ceph tell mon.b version
  expect_false ceph tell mon.foo version

  sleep 1

  ceph_watch_start debug
  ceph tell mon.a version
  ceph_watch_wait 'mon.0 \[DBG\] from.*cmd=\[{"prefix": "version"}\]: dispatch'

  ceph_watch_start debug
  ceph tell mon.b version
  ceph_watch_wait 'mon.1 \[DBG\] from.*cmd=\[{"prefix": "version"}\]: dispatch'
}

function test_mon_crushmap_validation()
{
  local map=$TMPDIR/map
  ceph osd getcrushmap -o $map

  local crushtool_path="${TMPDIR}/crushtool"
  touch "${crushtool_path}"
  chmod +x "${crushtool_path}"
  local crushtool_path_old=`ceph-conf --show-config-value crushtool`
  ceph tell mon.\* injectargs --crushtool "${crushtool_path}"

  printf "%s\n" \
      "#!/bin/sh
       cat > /dev/null
       exit 0" > "${crushtool_path}"

  ceph osd setcrushmap -i $map

  printf "%s\n" \
      "#!/bin/sh
       cat > /dev/null
       exit 1" > "${crushtool_path}"

  expect_false ceph osd setcrushmap -i $map

  printf "%s\n" \
      "#!/bin/sh
       cat > /dev/null
       echo 'TEST FAIL' >&2
       exit 1" > "${crushtool_path}"

  expect_false ceph osd setcrushmap -i $map 2> $TMPFILE
  check_response "Error EINVAL: Failed crushmap test: TEST FAIL"

  local mon_lease=`ceph-conf --show-config-value mon_lease`

  test "${mon_lease}" -gt 0

  printf "%s\n" \
      "#!/bin/sh
       cat > /dev/null
       sleep $((mon_lease - 1))" > "${crushtool_path}"

  ceph osd setcrushmap -i $map

  printf "%s\n" \
      "#!/bin/sh
       cat > /dev/null
       sleep $((mon_lease + 1))" > "${crushtool_path}"

  expect_false ceph osd setcrushmap -i $map 2> $TMPFILE
  check_response "Error EINVAL: Failed crushmap test: ${crushtool_path}: timed out (${mon_lease} sec)"

  ceph tell mon.\* injectargs --crushtool "${crushtool_path_old}"

  rm -f "${crushtool_path}"
}

function test_mon_ping()
{
  ceph ping mon.a
  ceph ping mon.b
  expect_false ceph ping mon.foo

  ceph ping mon.\*
}

function test_mon_deprecated_commands()
{
  # current DEPRECATED commands are:
  #  ceph compact
  #  ceph scrub
  #  ceph sync force
  #
  # Testing should be accomplished by setting
  # 'mon_debug_deprecated_as_obsolete = true' and expecting ENOTSUP for
  # each one of these commands.

  ceph tell mon.a injectargs '--mon-debug-deprecated-as-obsolete'
  expect_false ceph tell mon.a compact 2> $TMPFILE
  check_response "\(EOPNOTSUPP\|ENOTSUP\): command is obsolete"

  expect_false ceph tell mon.a scrub 2> $TMPFILE
  check_response "\(EOPNOTSUPP\|ENOTSUP\): command is obsolete"

  expect_false ceph tell mon.a sync force 2> $TMPFILE
  check_response "\(EOPNOTSUPP\|ENOTSUP\): command is obsolete"

  ceph tell mon.a injectargs '--no-mon-debug-deprecated-as-obsolete'
}

function test_mon_cephdf_commands()
{
  # ceph df detail:
  # pool section:
  # RAW USED The near raw used per pool in raw total

  ceph osd pool create cephdf_for_test 32 32 replicated
  ceph osd pool set cephdf_for_test size 2

  dd if=/dev/zero of=./cephdf_for_test bs=4k count=1
  rados put cephdf_for_test cephdf_for_test -p cephdf_for_test

  #wait for update
  sleep 10

  cal_raw_used_size=`ceph df detail | grep cephdf_for_test | awk -F ' ' '{printf "%d\n", 2 * $4}'`
  raw_used_size=`ceph df detail | grep cephdf_for_test | awk -F ' '  '{print $11}'`

  ceph osd pool delete cephdf_for_test cephdf_for_test --yes-i-really-really-mean-it
  rm ./cephdf_for_test

  expect_false test $cal_raw_used_size != $raw_used_size
}

#
# New tests should be added to the TESTS array below
#
# Individual tests may be run using the '-t <testname>' argument
# The user can specify '-t <testname>' as many times as she wants
#
# Tests will be run in order presented in the TESTS array, or in
# the order specified by the '-t <testname>' options.
#
# '-l' will list all the available test names
# '-h' will show usage
#
# The test maintains backward compatibility: not specifying arguments
# will run all tests following the order they appear in the TESTS array.
#

set +x
MON_TESTS+=" mon_injectargs"
MON_TESTS+=" mon_injectargs_SI"
MON_TESTS+=" tiering"
MON_TESTS+=" auth"
MON_TESTS+=" auth_profiles"
MON_TESTS+=" mon_misc"
MON_TESTS+=" mon_mon"
MON_TESTS+=" mon_osd"
MON_TESTS+=" mon_osd_pool"
MON_TESTS+=" mon_osd_pool_quota"
MON_TESTS+=" mon_pg"
MON_TESTS+=" mon_osd_pool_set"
MON_TESTS+=" mon_osd_tiered_pool_set"
MON_TESTS+=" mon_osd_erasure_code"
MON_TESTS+=" mon_osd_misc"
MON_TESTS+=" mon_heap_profiler"
MON_TESTS+=" mon_tell"
MON_TESTS+=" mon_crushmap_validation"
MON_TESTS+=" mon_ping"
MON_TESTS+=" mon_deprecated_commands"
MON_TESTS+=" mon_caps"
OSD_TESTS+=" osd_bench"
OSD_TESTS+=" osd_negative_filestore_merge_threshold"
OSD_TESTS+=" tiering_agent"

MDS_TESTS+=" mds_tell"
MDS_TESTS+=" mon_mds"
MDS_TESTS+=" mon_mds_metadata"
MDS_TESTS+=" mon_cephdf_commands"

TESTS+=$MON_TESTS
TESTS+=$OSD_TESTS
TESTS+=$MDS_TESTS

#
# "main" follows
#

function list_tests()
{
  echo "AVAILABLE TESTS"
  for i in $TESTS; do
    echo "  $i"
  done
}

function usage()
{
  echo "usage: $0 [-h|-l|-t <testname> [-t <testname>...]]"
}

tests_to_run=()

sanity_check=true

while [[ $# -gt 0 ]]; do
  opt=$1

  case "$opt" in
    "-l" )
      do_list=1
      ;;
    "--asok-does-not-need-root" )
      SUDO=""
      ;;
    "--no-sanity-check" )
      sanity_check=false
      ;;
    "--test-mon" )
      tests_to_run+="$MON_TESTS"
      ;;
    "--test-osd" )
      tests_to_run+="$OSD_TESTS"
      ;;
    "--test-mds" )
      tests_to_run+="$MDS_TESTS"
      ;;
    "-t" )
      shift
      if [[ -z "$1" ]]; then
        echo "missing argument to '-t'"
        usage ;
        exit 1
      fi
      tests_to_run+=" $1"
      ;;
    "-h" )
      usage ;
      exit 0
      ;;
  esac
  shift
done

if [[ $do_list -eq 1 ]]; then
  list_tests ;
  exit 0
fi

if test -z "$tests_to_run" ; then
  tests_to_run="$TESTS"
fi

if $sanity_check ; then
    wait_no_osd_down
fi
for i in $tests_to_run; do
  if $sanity_check ; then
      check_no_osd_down
  fi
  set -x
  test_${i}
  set +x
done
if $sanity_check ; then
    check_no_osd_down
fi

set -x

echo OK
