#!/bin/bash
ps cax | grep redis-server > /dev/null
if [ $? -eq 0 ];
then 
	echo "Redis process found; flushing!"
	redis-cli FLUSHALL
fi
redis-server --daemonize yes
echo "-----------Redis Server Started-----------"
../../../build/bin/ceph_test_rgw_d4n_directory
printf "\n-----------Directory Test Executed-----------\n"
redis-cli FLUSHALL
echo "-----------Redis Server Flushed-----------"
../../../build/bin/ceph_test_rgw_d4n_filter
printf "\n-----------Filter Test Executed-----------\n"
redis-cli FLUSHALL
echo "-----------Redis Server Flushed-----------"
REDIS_PID=$(lsof -i4TCP:6379 -sTCP:LISTEN -t)
kill $REDIS_PID
echo "-----------Redis Server Stopped-----------"
