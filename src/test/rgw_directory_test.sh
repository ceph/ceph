#!/bin/bash
#PORT_NUM=$(./port_scanner.sh 127.0.0.1 40000 65535 | grep open | grep [0-9]* -o | cut -d ' ' -f1)
PORT_NUM=50104
echo "-----------Port $PORT_NUM Discovered-----------"
redis-server --port $PORT_NUM --daemonize yes
echo "-----------Redis Server Started-----------"
../../build/bin/ceph_test_rgw_directory $PORT_NUM
printf "\n-----------Directory Test Executed; Showing Directory Values-----------\n"
echo "keys *" | redis-cli -p $PORT_NUM
redis-cli -h 127.0.0.1 -p ${PORT_NUM[0]} FLUSHALL
echo "-----------Redis Server Flushed-----------"
REDIS_PID=$(lsof -i4TCP:$PORT_NUM -sTCP:LISTEN -t)
kill $REDIS_PID
echo "-----------Redis Server Stopped-----------"
