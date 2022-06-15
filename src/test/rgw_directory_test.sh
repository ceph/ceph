#!/bin/bash
#PORT_NUM=$(./port_scanner.sh 127.0.0.1 40000 65535 | grep closed | awk '{ print $1 }' | head -1)
PORT_NUM=6379
echo "-----------Port $PORT_NUM Discovered-----------"
redis-server --port $PORT_NUM --daemonize yes
echo "-----------Redis Server Started-----------"

timeout 2s ../../build/bin/ceph_test_rgw_directory $PORT_NUM

if (($? == 124)); then
  echo "getValue timed out; restarting script"
  
  redis-cli -h 127.0.0.1 -p $PORT_NUM FLUSHALL
  REDIS_PID=$(lsof -i4TCP:$PORT_NUM -sTCP:LISTEN -t)
  kill $REDIS_PID
  source ./rgw_directory_test.sh
else
  printf "\n-----------Directory Test Executed; Showing Directory Values-----------\n"
  echo "keys *" | redis-cli -p $PORT_NUM
  redis-cli -h 127.0.0.1 -p $PORT_NUM FLUSHALL
  echo "-----------Redis Server Flushed-----------"
  REDIS_PID=$(lsof -i4TCP:$PORT_NUM -sTCP:LISTEN -t)
  kill $REDIS_PID
  echo "-----------Redis Server Stopped-----------"
fi
