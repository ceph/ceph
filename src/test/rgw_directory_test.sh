#!/bin/bash
redis-server --daemonize yes
echo "-----------Redis Server Started-----------"
timeout 5s ../../build/bin/ceph_test_rgw_directory || echo "Failed from timeout"
printf "\n-----------Directory Test Executed-----------\n"
redis-cli FLUSHALL
echo "-----------Redis Server Flushed-----------"
REDIS_PID=$(lsof -i4TCP:6379 -sTCP:LISTEN -t)
kill $REDIS_PID
sleep 0.5
echo "-----------Redis Server Stopped-----------"
