#!/bin/bash -x

set -e

get_pg()
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

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

#
# Assumes there are at least 3 MDSes and two OSDs
#

ceph auth add client.xx mon allow osd "allow *"
ceph auth list | grep client.xx
ceph auth get client.xx | grep caps | grep mon
ceph auth get client.xx | grep caps | grep osd
ceph auth get-key client.xx
ceph auth print-key client.xx
ceph auth print_key client.xx
ceph auth caps client.xx osd "allow rw"
expect_false "(ceph auth get client.xx | grep caps | grep mon)"
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
ceph --verbose osd dump | grep '^epoch'

# df
ceph df | grep GLOBAL
ceph df detail | grep CATEGORY
ceph df --format json | grep 'total_space'
ceph df detail --format json | grep 'rd_kb'
ceph df --format xml | grep '<total_space>'
ceph df detail --format xml | grep '<rd_kb>'

ceph fsid
ceph health
ceph health detail
ceph health --format json-pretty
ceph health detail --format xml-pretty

ceph injectargs -- --debug_ms=1
ceph injectargs -- --debug-ms=1
expect_false ceph injectargs -- debug_ms=1
mymsg="this is a test log message $$.$(date)"
ceph log "$mymsg"
logfile=$(ceph-conf --name=mon.a --lookup log_file)
grep "$mymsg" $logfile
ceph mds cluster_down 2>&1 | grep "marked mdsmap DOWN"
expect_false ceph mds cluster_down
ceph mds cluster_up 2>&1 | grep "unmarked mdsmap DOWN"
expect_false ceph mds cluster_up

# XXX is this a reasonable test?
ceph mds compat rm_incompat 5
expect_false ceph mds rm_compat 5

ceph mds compat show
expect_false ceph mds deactivate 2
ceph mds dump
# XXX mds fail, but how do you undo it?
mdsmapfile=/tmp/mdsmap.$$
current_epoch=$(ceph mds getmap -o $mdsmapfile 2>&1 | sed 's/.*epoch //')
[ -s $mdsmapfile ]
((epoch = current_epoch + 1))
ceph mds setmap -i $mdsmapfile $epoch
rm $mdsmapfile

ceph mds newfs 0 1 --yes-i-really-mean-it
ceph osd pool create data2 10
poolnum=$(ceph osd dump | grep 'pool.*data2' | awk '{print $2;}')
ceph mds add_data_pool $poolnum
ceph mds remove_data_pool $poolnum
ceph osd pool delete data2 data2 --yes-i-really-really-mean-it
ceph mds set_max_mds 4
ceph mds set_max_mds 3
ceph mds stat
# ceph mds tell mds.a getmap
# ceph mds rm
# ceph mds rmfailed
# ceph mds set_state
# ceph mds stop
# ceph mds tell

# no mon add/remove
ceph mon dump
ceph mon getmap -o /tmp/monmap
[ -s /tmp/monmap ]
# ceph mon tell
ceph mon_status

bl=192.168.0.1:0/1000
ceph osd blacklist add $bl
ceph osd blacklist ls | grep $bl
ceph osd blacklist rm $bl
expect_false "(ceph osd blacklist ls | grep $bl)"

ceph osd crush tunables legacy
ceph osd crush tunables bobtail

# how do I tell when these are done?
#ceph osd scrub 0
#ceph osd deep-scrub 0
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
ceph osd dump | grep 'osd.0 up'
ceph osd find 1
ceph osd out 0
ceph osd dump | grep 'osd.0.*out'
ceph osd in 0
ceph osd dump | grep 'osd.0.*in'
ceph osd find 0

f=/tmp/map.$$
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

#XXX ceph osd lost - this is bad, right?
ceph osd ls
ceph osd lspools | grep data
ceph osd map data foo | grep 'pool.*data.*object.*foo.*pg.*up.*acting'

ceph osd pause 0
ceph osd dump | grep 'flags pauserd,pausewr'
ceph osd unpause 0

ceph osd tree

ceph osd pool mksnap data datasnap
rados -p data lssnap | grep datasnap
ceph osd pool rmsnap data datasnap

ceph osd pool create data2 10
ceph osd pool rename data2 data3
ceph osd lspools | grep data3
ceph osd pool delete data3 data3 --yes-i-really-really-mean-it

# XXX ceph osd repair
ceph osd stat | grep up,
# XXX ceph osd tell

# XXX does this leave osds out/down?
# ceph osd thrash 10


ceph pg debug unfound_objects_exist
ceph pg debug degraded_pgs_exist
ceph pg deep-scrub 0.0
ceph pg dump
ceph pg dump_json
ceph pg dump_pools_json
ceph pg dump_stuck inactive
ceph pg dump_stuck unclean
ceph pg dump_stuck stale
# XXX can I test this?
# ceph pg force_create_pg
ceph pg getmap -o /tmp/map
[ -s /tmp/map ]
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
ceph quorum enter
ceph quorum_status
ceph report | grep osd_stats
ceph status
ceph -s
# ceph stop_cluster
# ceph sync force
ceph sync status | grep paxos_version

ceph tell osd.0 version
expect_false ceph tell osd.9999 version 
expect_false ceph tell osd.foo version

ceph tell osd.0 dump_pg_recovery_stats | grep Started

ceph osd reweight 0 0.9
expect_false ceph osd reweight 0 -1
ceph osd reweight 0 1

for s in pg_num pgp_num size min_size crash_replay_interval crush_ruleset; do
	ceph osd pool get data $s
done

ceph osd pool get data size | grep 'size: 2'
ceph osd pool set data size 3
ceph osd pool get data size | grep 'size: 3'
ceph osd pool set data size 2

ceph osd pool get rbd crush_ruleset | grep 'crush_ruleset: 2'

for id in `ceph osd ls` ; do
	ceph tell osd.$id version
done

echo OK
