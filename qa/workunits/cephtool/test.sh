#!/bin/bash -x

set -e
set -o functrace
PS4=' ${FUNCNAME[0]}: $LINENO: '
SUDO=sudo

function get_pg()
{
	local pool obj map_output pg
	pool=$1
	obj=$2
	declare -a map_output
	map_output=($(ceph osd map $1 $2))
	for (( i=0; i<${#map_output[*]}; i++ )) ; do
		if [ "${map_output[$i]}" == "pg" ] ; then
			pg=${map_output[((i+2))]}
			break
		fi
	done
	pg=$(echo $pg | sed 's/[()]//g')
	echo $pg
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

function check_response()
{
	expected_stderr_string=$1
	retcode=$2
	expected_retcode=$3
	if [ "$expected_retcode" -a $retcode != $expected_retcode ] ; then
		echo "return code invalid: got $retcode, expected $expected_retcode" >&2
		exit 1
	fi

	if ! grep "$expected_stderr_string" $TMPFILE >/dev/null 2>&1 ; then 
		echo "Didn't find $expected_stderr_string in stderr output" >&2
		echo "Stderr: " >&2
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
  expect_false ceph injectargs mon.a '--mon_pg_warn_min_objects 10F'
  $SUDO ceph daemon mon.a config set mon_pg_warn_min_objects $initial_value
}

function test_tiering()
{
  # tiering
  ceph osd pool create cache 2
  ceph osd pool create cache2 2
  ceph osd tier add data cache
  ceph osd tier add data cache2
  expect_false ceph osd tier add metadata cache
  # test some state transitions
  ceph osd tier cache-mode cache writeback
  ceph osd tier cache-mode cache forward
  ceph osd tier cache-mode cache readonly
  ceph osd tier cache-mode cache forward
  ceph osd tier cache-mode cache none
  ceph osd tier cache-mode cache writeback
  expect_false ceph osd tier cache-mode cache none
  expect_false ceph osd tier cache-mode cache readonly
  # test with dirty objects in the tier pool
  # tier pool currently set to 'writeback'
  rados -p cache put /etc/passwd /etc/passwd
  ceph tell osd.* flush_pg_stats || true
  # 1 dirty object in pool 'cache'
  ceph osd tier cache-mode cache forward
  expect_false ceph osd tier cache-mode cache none
  expect_false ceph osd tier cache-mode cache readonly
  ceph osd tier cache-mode cache writeback
  # remove object from tier pool
  rados -p cache rm /etc/passwd
  rados -p cache cache-flush-evict-all
  ceph tell osd.* flush_pg_stats || true
  # no dirty objects in pool 'cache'
  ceph osd tier cache-mode cache forward
  ceph osd tier cache-mode cache none
  ceph osd tier cache-mode cache readonly
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
  ceph osd tier set-overlay data cache
  expect_false ceph osd tier set-overlay data cache2
  expect_false ceph osd tier remove data cache
  ceph osd tier remove-overlay data
  ceph osd tier set-overlay data cache2
  ceph osd tier remove-overlay data
  ceph osd tier remove data cache
  ceph osd tier add metadata cache
  expect_false ceph osd tier set-overlay data cache
  ceph osd tier set-overlay metadata cache
  ceph osd tier remove-overlay metadata
  ceph osd tier remove metadata cache
  ceph osd tier remove data cache2

  # make sure a non-empty pool fails
  rados -p cache2 put /etc/passwd /etc/passwd
  while ! ceph df | grep cache2 | grep ' 1 ' ; do
    echo waiting for pg stats to flush
    sleep 2
  done
  expect_false ceph osd tier add data cache2
  ceph osd tier add data cache2 --force-nonempty
  ceph osd tier remove data cache2

  ceph osd pool delete cache cache --yes-i-really-really-mean-it
  ceph osd pool delete cache2 cache2 --yes-i-really-really-mean-it

  # make sure we can't create an ec pool tier
  ceph osd pool create eccache 2 2 erasure
  ceph osd pool create repbase 2
  expect_false ceph osd tier add repbase eccache
  ceph osd pool delete repbase repbase --yes-i-really-really-mean-it
  ceph osd pool delete eccache eccache --yes-i-really-really-mean-it

  # convenient add-cache command
  ceph osd pool create cache3 2
  ceph osd tier add-cache data cache3 1024000
  ceph osd dump | grep cache3 | grep bloom | grep 'false_positive_probability: 0.05' | grep 'target_bytes 1024000' | grep '1200s x4'
  ceph osd tier remove data cache3
  ceph osd pool delete cache3 cache3 --yes-i-really-really-mean-it

  ceph osd pool delete slow2 slow2 --yes-i-really-really-mean-it
  ceph osd pool delete slow slow --yes-i-really-really-mean-it

  # protection against pool removal when used as tiers
  ceph osd pool create datapool 2
  ceph osd pool create cachepool 2
  ceph osd tier add-cache datapool cachepool 1024000
  ceph osd pool delete cachepool cachepool --yes-i-really-really-mean-it 2> $TMPFILE || true
  check_response "EBUSY: pool 'cachepool' is a tier of 'datapool'"
  ceph osd pool delete datapool datapool --yes-i-really-really-mean-it 2> $TMPFILE || true
  check_response "EBUSY: pool 'datapool' has tiers cachepool"
  ceph osd tier remove datapool cachepool
  ceph osd pool delete cachepool cachepool --yes-i-really-really-mean-it
  ceph osd pool delete datapool datapool --yes-i-really-really-mean-it

  # check health check
  ceph osd pool create datapool 2
  ceph osd pool create cache4 2
  ceph osd tier add datapool cache4
  ceph osd pool set cache4 target_max_objects 5
  ceph osd pool set cache4 target_max_bytes 1000
  for f in `seq 1 5` ; do
    rados -p cache4 put foo$f /etc/passwd
  done
  while ! ceph df | grep cache4 | grep ' 5 ' ; do
    echo waiting for pg stats to flush
    sleep 2
  done
  ceph health | grep WARN | grep cache4
  ceph health detail | grep cache4 | grep 'target max' | grep objects
  ceph health detail | grep cache4 | grep 'target max' | grep 'B'
  ceph osd tier remove datapool cache4
  ceph osd pool delete cache4 cache4 --yes-i-really-really-mean-it
  ceph osd pool delete datapool datapool --yes-i-really-really-mean-it

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
  ! ceph osd dump | grep "pool.*'cache5'" 2>&1 | grep "tier_of"
  ceph osd tier remove basepoolB cache6 2>&1 | grep 'not a tier of'
  ! ceph osd dump | grep "pool.*'cache6'" 2>&1 | grep "tier_of"

  ! ceph osd dump | grep "pool.*'basepoolA'" 2>&1 | grep "tiers"
  ! ceph osd dump | grep "pool.*'basepoolB'" 2>&1 | grep "tiers"

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
  grep 'total_space' $TMPFILE
  grep -v 'dirty' $TMPFILE
  ceph df detail --format json > $TMPFILE
  grep 'rd_kb' $TMPFILE
  grep 'dirty' $TMPFILE
  ceph df --format xml | grep '<total_space>'
  ceph df detail --format xml | grep '<rd_kb>'

  ceph fsid
  ceph health
  ceph health detail
  ceph health --format json-pretty
  ceph health detail --format xml-pretty

  ceph -w > $TMPDIR/$$ &
  wpid="$!"
  mymsg="this is a test log message $$.$(date)"
  ceph log "$mymsg"
  sleep 3
  if ! grep "$mymsg" $TMPDIR/$$; then
    # in case it is very slow (mon thrashing or something)
    sleep 30
    grep "$mymsg" $TMPDIR/$$
  fi
  kill $wpid
}


function test_mon_mds()
{
ceph mds cluster_down
ceph mds cluster_up

ceph mds compat rm_incompat 4
ceph mds compat rm_incompat 4

ceph mds compat show
expect_false ceph mds deactivate 2
ceph mds dump
# XXX mds fail, but how do you undo it?
mdsmapfile=$TMPDIR/mdsmap.$$
current_epoch=$(ceph mds getmap -o $mdsmapfile --no-log-to-stderr 2>&1 | grep epoch | sed 's/.*epoch //')
[ -s $mdsmapfile ]
((epoch = current_epoch + 1))
ceph mds setmap -i $mdsmapfile $epoch
rm $mdsmapfile

ceph mds newfs 0 1 --yes-i-really-mean-it
ceph osd pool create data2 10
poolnum=$(ceph osd dump | grep 'pool.*data2' | awk '{print $2;}')
ceph mds add_data_pool $poolnum
ceph mds add_data_pool rbd
ceph mds remove_data_pool $poolnum
ceph mds remove_data_pool rbd
ceph osd pool delete data2 data2 --yes-i-really-really-mean-it
ceph mds set_max_mds 4
ceph mds set_max_mds 3
ceph mds set max_mds 4
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

ceph mds stat
# ceph mds tell mds.a getmap
# ceph mds rm
# ceph mds rmfailed
# ceph mds set_state
# ceph mds stop
}

function test_mon_mon()
{
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
  ceph osd blacklist add $bl
  ceph osd blacklist ls | grep $bl
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

  for f in noup nodown noin noout noscrub nodeep-scrub nobackfill norecover notieragent
  do
    ceph osd set $f
    ceph osd unset $f
  done
  expect_false ceph osd set bogus
  expect_false ceph osd unset bogus

  ceph osd set noup
  ceph osd down 0
  ceph osd dump | grep 'osd.0 down'
  ceph osd unset noup
  for ((i=0; i < 100; i++)); do
    if ! ceph osd dump | grep 'osd.0 up'; then
      echo "waiting for osd.0 to come back up"
      sleep 10
    else
      break
    fi
  done

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
  rm $f
  ceph osd getmap -o $f
  [ -s $f ]
  rm $f
  save=$(ceph osd getmaxosd | sed -e 's/max_osd = //' -e 's/ in epoch.*//')
  ceph osd setmaxosd 10
  ceph osd getmaxosd | grep 'max_osd = 10'
  ceph osd setmaxosd $save
  ceph osd getmaxosd | grep "max_osd = $save"

  for id in `ceph osd ls` ; do
    ceph tell osd.$id version
  done

  ceph osd rm 0 2>&1 | grep 'EBUSY'

  id=`ceph osd create`
  ceph osd lost $id --yes-i-really-mean-it
  ceph osd rm $id

  uuid=`uuidgen`
  id=`ceph osd create $uuid`
  id2=`ceph osd create $uuid`
  [ "$id" = "$id2" ]
  ceph osd rm $id

  ceph osd ls
  ceph osd lspools | grep data
  ceph osd map data foo | grep 'pool.*data.*object.*foo.*pg.*up.*acting'

  ceph osd pause
  ceph osd dump | grep 'flags pauserd,pausewr'
  ceph osd unpause

  ceph osd tree

  ceph osd stat | grep up,
}

function test_mon_osd_pool()
{
  #
  # osd pool
  #
  ceph osd pool mksnap data datasnap
  rados -p data lssnap | grep datasnap
  ceph osd pool rmsnap data datasnap

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
  # can't test this...
  # ceph pg force_create_pg
  ceph pg getmap -o $TMPDIR/map.$$
  [ -s $TMPDIR/map.$$ ]
  ceph pg map 0.0 | grep acting
  ceph pg repair 0.0
  ceph pg scrub 0.0

  ceph pg send_pg_creates
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
  for s in pg_num pgp_num size min_size crash_replay_interval crush_ruleset; do
    ceph osd pool get data $s
  done

  old_size=$(ceph osd pool get data size | sed -e 's/size: //')
  (( new_size = old_size + 1 ))
  ceph osd pool set data size $new_size
  ceph osd pool get data size | grep "size: $new_size"
  ceph osd pool set data size $old_size

  ceph osd pool create pool_erasure 12 12 erasure
  set +e
  ceph osd pool set pool_erasure size 4444 2>$TMPFILE
  check_response 'not change the size'
  set -e
  ceph osd pool get pool_erasure erasure_code_profile

  auid=5555
  ceph osd pool set data auid $auid
  ceph osd pool get data auid | grep $auid
  ceph --format=xml osd pool get data auid | grep $auid
  ceph osd pool set data auid 0

  ceph osd pool set data hashpspool true
  ceph osd pool set data hashpspool false
  ceph osd pool set data hashpspool 0
  ceph osd pool set data hashpspool 1
  expect_false ceph osd pool set data hashpspool asdf
  expect_false ceph osd pool set data hashpspool 2

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
  ceph osd reweight-by-utilization 80 2>$TMPFILE; check_response 'not in range' $? 22
  set -e
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
TESTS=(
  mon_injectargs_SI
  tiering
  auth
  mon_misc
  mon_mds
  mon_mon
  mon_osd
  mon_osd_pool
  mon_osd_pool_quota
  mon_pg
  mon_osd_pool_set
  mon_osd_tiered_pool_set
  mon_osd_erasure_code
  mon_osd_misc
  mon_heap_profiler
  osd_bench
)

#
# "main" follows
#

function list_tests()
{
  echo "AVAILABLE TESTS"
  for i in ${TESTS[@]}; do
    echo "  $i"
  done
}

function usage()
{
  echo "usage: $0 [-h|-l|-t <testname> [-t <testname>...]]"
}

tests_to_run=()

while [[ $# -gt 0 ]]; do
  opt=$1

  case "$opt" in
    "-l" )
      do_list=1
      ;;
    "--asok-does-not-need-root" )
      SUDO=""
      ;;
    "-t" )
      shift
      if [[ -z "$1" ]]; then
        echo "missing argument to '-t'"
        usage ;
        exit 1
      fi
      tests_to_run=("${tests_to_run[@]}" "$1")
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

if [[ ${#tests_to_run[@]} -eq 0 ]]; then
  tests_to_run=("${TESTS[@]}")
fi

for i in ${tests_to_run[@]}; do
  set -x
  test_${i} ;
  set +x
done

set -x

echo OK
