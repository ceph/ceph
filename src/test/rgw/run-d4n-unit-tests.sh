#!/bin/bash
ps cax | grep redis-server > /dev/null
if [ $? -eq 0 ];
then 
	echo "Redis process found; flushing!"
	redis-cli FLUSHALL
else
redis-server --daemonize yes
fi

../../../build/bin/ceph_test_rgw_d4n_directory
printf "\n-----------Directory Test Executed-----------\n"

redis-cli FLUSHALL
../../../build/bin/ceph_test_rgw_d4n_policy
printf "\n-----------Policy Test Executed-----------\n"

redis-cli FLUSHALL
../../../build/bin/ceph_test_rgw_redis_driver
printf "\n-----------RedisDriver Test Executed-----------\n"

REDIS_PID=$(lsof -i4TCP:6379 -sTCP:LISTEN -t)
kill $REDIS_PID
echo "-----------Redis Server Stopped-----------"
