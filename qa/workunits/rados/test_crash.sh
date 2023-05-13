#!/bin/sh

set -x

# run on a single-node three-OSD cluster

sudo killall -ABRT ceph-osd
sleep 5

# kill caused coredumps; find them and delete them, carefully, so as
# not to disturb other coredumps, or else teuthology will see them
# and assume test failure.  sudos are because the core files are
# root/600
for f in $(find $TESTDIR/archive/coredump -type f); do
	gdb_output=$(echo "quit" | sudo gdb /usr/bin/ceph-osd $f)
	if expr match "$gdb_output" ".*generated.*ceph-osd.*" && \
	   ( \

	   	expr match "$gdb_output" ".*terminated.*signal 6.*" || \
	   	expr match "$gdb_output" ".*terminated.*signal SIGABRT.*" \
	   )
	then
		sudo rm $f
	fi
done

# ceph-crash runs as the unprivileged "ceph" user, but when under test
# the ceph osd daemons are running as root, so their crash files aren't
# readable.  let's chown them so they behave as they would in real life.
sudo chown -R ceph:ceph /var/lib/ceph/crash

# let daemon find crashdumps on startup
sudo systemctl restart ceph-crash
sleep 30

# must be 3 crashdumps registered and moved to crash/posted
[ $(ceph crash ls | wc -l) = 4 ]  || exit 1   # 4 here bc of the table header
[ $(sudo find /var/lib/ceph/crash/posted/ -name meta | wc -l) = 3 ] || exit 1

# there should be a health warning
ceph health detail | grep RECENT_CRASH || exit 1
ceph crash archive-all
sleep 30
ceph health detail | grep -c RECENT_CRASH | grep 0     # should be gone!
