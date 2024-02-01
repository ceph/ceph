#!/bin/bash

# checks if the containers default pids-limit (4096) is removed and Iscsi
# containers continue to run
# exits 1 if fails

set -ex

ISCSI_CONT_IDS=$(sudo podman ps -qa --filter='name=iscsi')
CONT_COUNT=$(echo ${ISCSI_CONT_IDS} | wc -w)
test ${CONT_COUNT} -eq 2

for i in ${ISCSI_CONT_IDS}
do
  # cgroups v1 and v2 have slightly different file locations for the pids.max
  # so check both spots
  if [ $(sudo podman exec ${i} cat /sys/fs/cgroup/pids/pids.max) ]; then
    pid_limit=$(sudo podman exec ${i} cat /sys/fs/cgroup/pids/pids.max)
  elif [ $(sudo podman exec ${i} cat /sys/fs/cgroup/pids.max) ]; then
    pid_limit=$(sudo podman exec ${i} cat /sys/fs/cgroup/pids.max)
  else
    echo "could not find pids.max inside container"
    exit 1
  fi
  test $pid_limit == max
done

for i in ${ISCSI_CONT_IDS}
do
  sudo podman exec ${i} /bin/sh -c 'for j in {0..20000}; do sleep 300 & done'
done

for i in ${ISCSI_CONT_IDS}
do
  SLEEP_COUNT=$(sudo podman exec ${i} /bin/sh -c 'ps -ef | grep -c sleep')
  test ${SLEEP_COUNT} -gt 20000
done

echo OK
