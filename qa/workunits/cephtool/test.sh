#!/bin/bash -x

set -e
set -o functrace
PS4=' ${FUNCNAME[0]}: $LINENO: '

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


# tiering
ceph osd pool create cache 2
ceph osd pool create cache2 2
ceph osd tier add data cache
ceph osd tier add data cache2
expect_false ceph osd tier add metadata cache
ceph osd tier cache-mode cache writeback
ceph osd tier cache-mode cache readonly
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

# convenient add-cache command
ceph osd pool create cache3 2
ceph osd tier add-cache data cache3 1024000
ceph osd dump | grep cache3 | grep bloom | grep 'false_positive_probability: 0.05' | grep 'target_bytes 1024000' | grep '1200s x4'
ceph osd tier remove data cache3
ceph osd pool delete cache3 cache3 --yes-i-really-really-mean-it

# check health check
ceph osd pool create cache4 2
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
ceph osd pool delete cache4 cache4 --yes-i-really-really-mean-it

# Assumes there are at least 3 MDSes and two OSDs
#

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

# no mon add/remove
ceph mon dump
ceph mon getmap -o $TMPDIR/monmap.$$
[ -s $TMPDIR/monmap.$$ ]
# ceph mon tell
ceph mon_status

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

ceph osd crush tunables legacy
ceph osd crush show-tunables | grep argonaut
ceph osd crush tunables bobtail
ceph osd crush show-tunables | grep bobtail
ceph osd crush tunables firefly
ceph osd crush show-tunables | grep firefly

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

ceph osd thrash 10
ceph osd down `seq 0 31`  # force everything down so that we can trust up
# make sure everything gets back up+in.
for ((i=0; i < 100; i++)); do
	if ceph osd dump | grep ' down '; then
		echo "waiting for osd(s) to come back up"
		sleep 10
	else
		break
	fi
done
# if you have more osds than this you are on your own
for f in `seq 0 31`; do
    ceph osd in $f || true
done

ceph osd dump | grep 'osd.0 up'
ceph osd find 1
ceph osd metadata 1 | grep 'distro'
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

ceph osd stat | grep up,

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
# ceph sync force

ceph tell osd.0 version
expect_false ceph tell osd.9999 version 
expect_false ceph tell osd.foo version

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

ceph osd pool set data hashpspool true
ceph osd pool set data hashpspool false
ceph osd pool set data hashpspool 0
ceph osd pool set data hashpspool 1
expect_false ceph osd pool set data hashpspool asdf
expect_false ceph osd pool set data hashpspool 2

ceph osd pool set rbd hit_set_type explicit_hash
ceph osd pool get rbd hit_set_type | grep "hit_set_type: explicit_hash"
ceph osd pool set rbd hit_set_type explicit_object
ceph osd pool get rbd hit_set_type | grep "hit_set_type: explicit_object"
ceph osd pool set rbd hit_set_type bloom
ceph osd pool get rbd hit_set_type | grep "hit_set_type: bloom"
expect_false ceph osd pool set rbd hit_set_type i_dont_exist
ceph osd pool set rbd hit_set_period 123
ceph osd pool get rbd hit_set_period | grep "hit_set_period: 123"
ceph osd pool set rbd hit_set_count 12
ceph osd pool get rbd hit_set_count | grep "hit_set_count: 12"
ceph osd pool set rbd hit_set_fpp .01
ceph osd pool get rbd hit_set_fpp | grep "hit_set_fpp: 0.01"

ceph osd pool set rbd target_max_objects 123
ceph osd pool set rbd target_max_bytes 123456
ceph osd pool set rbd cache_target_dirty_ratio .123
expect_false ceph osd pool set rbd cache_target_dirty_ratio -.2
expect_false ceph osd pool set rbd cache_target_dirty_ratio 1.1
ceph osd pool set rbd cache_target_full_ratio .123
ceph osd pool set rbd cache_target_full_ratio 1.0
ceph osd pool set rbd cache_target_full_ratio 0
expect_false ceph osd pool set rbd cache_target_full_ratio 1.1
ceph osd pool set rbd cache_min_flush_age 123
ceph osd pool set rbd cache_min_evict_age 234

ceph osd pool get rbd crush_ruleset | grep 'crush_ruleset: 0'


ceph osd erasure-code-profile set fooprofile a=b c=d
ceph osd erasure-code-profile set fooprofile a=b c=d
expect_false ceph osd erasure-code-profile set fooprofile a=b c=d e=f
ceph osd erasure-code-profile set fooprofile a=b c=d e=f --force
ceph osd erasure-code-profile set fooprofile a=b c=d e=f
expect_false ceph osd erasure-code-profile set fooprofile a=b c=d e=f g=h


set +e

# expect error about missing 'pool' argument
ceph osd map 2>$TMPFILE; check_response 'pool' $? 22

# expect error about unused argument foo
ceph osd ls foo 2>$TMPFILE; check_response 'unused' $? 22 

# expect "not in range" for invalid full ratio
ceph pg set_full_ratio 95 2>$TMPFILE; check_response 'not in range' $? 22

# expect "not in range" for invalid overload percentage
ceph osd reweight-by-utilization 80 2>$TMPFILE; check_response 'not in range' $? 22

# expect 'heap' commands to be correctly parsed
ceph heap stats
ceph heap start_profiler
ceph heap dump
ceph heap stop_profiler
ceph heap release


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
# max count: 409600

# more than max count must not be allowed
expect_false ceph tell osd.0 bench 409601 4096
# but 409600 must be succeed
ceph tell osd.0 bench 409600 4096

# for a large bs, we are limited by throughput.
# for a 2MB block size for 10 seconds, out max count is 50
# max count: 50

# more than max count must not be allowed
expect_false ceph tell osd.0 bench 51 2097152
# but 50 must succeed
ceph tell osd.0 bench 50 2097152


echo OK
